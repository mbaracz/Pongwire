package pl.mbaracz.jwebsockets;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import org.junit.jupiter.api.*;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assumptions.assumingThat;

class WebSocketServerTest {

    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            );
    }

    @Test
    void shouldAcceptWebSocketPathsStartingWithSlash() {
        assertThat(new WebSocketServer<>("/").getPath()).isEqualTo("/");
        assertThat(new WebSocketServer<>("/chat").getPath()).isEqualTo("/chat");
    }

    @Test
    void shouldRejectInvalidWebSocketPaths() {
        assertThatThrownBy(() -> new WebSocketServer<>(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("WebSocket path must start with '/' and must not contain query or fragment");
        assertThatThrownBy(() -> new WebSocketServer<>(""))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("WebSocket path must start with '/' and must not contain query or fragment");
        assertThatThrownBy(() -> new WebSocketServer<>("chat"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("WebSocket path must start with '/' and must not contain query or fragment");
        assertThatThrownBy(() -> new WebSocketServer<>("/chat?foo"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("WebSocket path must start with '/' and must not contain query or fragment");
        assertThatThrownBy(() -> new WebSocketServer<>("/chat#fragment"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("WebSocket path must start with '/' and must not contain query or fragment");
        assertThatThrownBy(() -> new WebSocketServer<>("/chat?foo#fragment"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("WebSocket path must start with '/' and must not contain query or fragment");
    }

    @Test
    void shouldFormatStartupAddressWithBoundPortAndPath() {
        WebSocketServer<String, Object> server = new WebSocketServer<>("/chat");

        assertThat(server.formatStartupAddress(new InetSocketAddress("0.0.0.0", 49152)))
            .isEqualTo("ws://0.0.0.0:49152/chat");
    }

    @Test
    void shouldFormatSecureStartupAddressWhenTlsIsConfigured() {
        WebSocketServer<String, Object> server = new WebSocketServer<String, Object>("/chat")
            .configure(configurer -> configurer.setTlsConfiguration(
                TlsConfiguration.forPem(Path.of("certificate.pem"), Path.of("private-key.pem"))
            ));

        assertThat(server.formatStartupAddress(new InetSocketAddress("0.0.0.0", 8443)))
            .isEqualTo("wss://0.0.0.0:8443/chat");
    }

    @Test
    void shouldThrowExceptionWhenServerIsAlreadyRunning() {
        server.listen(8080);

        assertThat(server.isRunning()).as("Server should be running").isTrue();
        assertThatThrownBy(() -> server.listen(8080)).as("Should throw exception").isInstanceOf(IllegalStateException.class);

        server.stop();
        assertThat(server.isRunning()).as("Server should not be running after stop").isFalse();
    }

    @Test
    void shouldThrowExceptionWhenServerIsNotRunning() {
        assertThat(server.isRunning()).as("Server should not be running").isFalse();
        assertThatThrownBy(() -> server.broadcast("foo")).as("Should throw exception").isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldExposeBoundLocalAddressOnlyWhileRunning() {
        assertThat(server.getLocalAddress()).as("Address before startup").isNull();

        server.listen(0);
        try {
            InetSocketAddress address = server.getLocalAddress();
            assertThat(address).as("Bound address").isNotNull();
            assertThat(address.getPort()).as("Ephemeral port selected by the operating system").isPositive();
        } finally {
            server.stop();
        }

        assertThat(server.getLocalAddress()).as("Address after shutdown").isNull();
    }

    @Test
    void shouldThrowExceptionWhenServerIsAlreadyStopped() throws IOException {
        // Start and stop the server
        server.listen(findFreePort());
        server.stop();

        assertThat(server.isRunning()).as("Server should not be running").isFalse();
        assertThatThrownBy(() -> server.stop()).as("Should throw exception").isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldThrowExceptionWhenEncoderIsNotProvided() {
        server.configure(configurer -> configurer.setMessageEncoder(null));

        assertThatThrownBy(() -> server.listen(8080)).as("Should throw exception").isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldThrowExceptionWhenDecoderIsNotProvided() {
        server.configure(configurer -> configurer.setMessageDecoder(null));

        assertThatThrownBy(() -> server.listen(8080)).as("Should throw exception").isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldThrowExceptionWhenPortIsAlreadyInUse() throws IOException {
        // Occupy a free port
        try (ServerSocket socket = new ServerSocket(0)) {
            // Assert startup fails instead of returning a server that is not running
            assertThatThrownBy(() -> server.listen(socket.getLocalPort())).as("Should throw exception").isInstanceOf(IllegalStateException.class);
            assertThat(server.isRunning()).as("Server should not be running").isFalse();
        }
    }

    @Test
    void shouldStartAgainOnTheSamePortWhenServerIsStopped() throws IOException {
        int port = findFreePort();

        server.listen(port);
        server.stop();

        // Assert port was released before stop() returned
        assertThatCode(() -> server.listen(port)).as("Server should start again on the same port").doesNotThrowAnyException();
        server.stop();
    }

    @Test
    @Timeout(30)
    void shouldReconnectRealClientAfterServerRestart() throws Exception {
        server.onMessage(WebSocketSession::sendMessage);
        WebSocket firstClient = null;
        WebSocket secondClient = null;

        try (HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build()) {
            server.listen(0);
            ClientListener firstListener = new ClientListener();
            firstClient = client.newWebSocketBuilder()
                .buildAsync(serverUri(), firstListener)
                .get(5, TimeUnit.SECONDS);

            assertThat(firstListener.opened.get(5, TimeUnit.SECONDS)).as("First connection should open").isTrue();
            assertThat(server.getConnectedSessions()).as("Sessions after first connection").hasSize(1);

            firstClient.sendText("first", true).get(5, TimeUnit.SECONDS);
            assertThat(firstListener.message.get(5, TimeUnit.SECONDS)).as("First echoed message").isEqualTo("first");

            server.stop();

            assertThat(firstListener.closeCode.get(5, TimeUnit.SECONDS))
                .as("First client should observe server shutdown")
                .isEqualTo(WebSocketCloseStatus.ENDPOINT_UNAVAILABLE.code());
            assertThat(server.getConnectedSessions()).as("Sessions after first shutdown").isEmpty();

            server.listen(0);
            ClientListener secondListener = new ClientListener();
            secondClient = client.newWebSocketBuilder()
                .buildAsync(serverUri(), secondListener)
                .get(5, TimeUnit.SECONDS);

            assertThat(secondListener.opened.get(5, TimeUnit.SECONDS)).as("Second connection should open").isTrue();
            assertThat(server.getConnectedSessions()).as("Sessions after reconnect").hasSize(1);

            secondClient.sendText("second", true).get(5, TimeUnit.SECONDS);
            assertThat(secondListener.message.get(5, TimeUnit.SECONDS)).as("Second echoed message").isEqualTo("second");

            server.stop();

            assertThat(secondListener.closeCode.get(5, TimeUnit.SECONDS))
                .as("Second client should observe server shutdown")
                .isEqualTo(WebSocketCloseStatus.ENDPOINT_UNAVAILABLE.code());
            assertThat(server.getConnectedSessions()).as("Sessions after second shutdown").isEmpty();
        } finally {
            if (firstClient != null) {
                firstClient.abort();
            }
            if (secondClient != null) {
                secondClient.abort();
            }
            stopIfNeeded(server);
        }
    }

    @Test
    void shouldCloseSessionsWhenServerIsStopped() throws IOException {
        server.listen(findFreePort());

        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        server.stop();

        // Assert client received a going away close frame and the connection was closed
        CloseWebSocketFrame frame = channel.readOutbound();
        assertThat(frame).as("Close frame should be sent").isNotNull();
        assertThat(frame.statusCode()).as("Should send going away status").isEqualTo(WebSocketCloseStatus.ENDPOINT_UNAVAILABLE.code());
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(server.getConnectedSessions()).as("Sessions should be removed").isEmpty();
        frame.release();
    }

    @Test
    void shouldReleaseResourcesOfFailedStartupWhenPortIsInvalid() throws IOException {
        // Open files can only be counted where /proc is available
        Path openFiles = Path.of("/proc/self/fd");
        assumeTrue(Files.isDirectory(openFiles), "Counting open files requires /proc");

        // Fail once first, so one-time initialization is not counted as leaked files
        assertThatThrownBy(() -> server.listen(-1)).as("Should reject invalid port").isInstanceOf(IllegalArgumentException.class);
        long openBefore = countFiles(openFiles);

        for (int i = 0; i < 5; i++) {
            assertThatThrownBy(() -> server.listen(-1)).as("Should reject invalid port").isInstanceOf(IllegalArgumentException.class);
        }

        long leaked = countFiles(openFiles) - openBefore;

        // Assert each failed startup released its event loops, which hold open selectors
        assertThat(server.isRunning()).as("Server should not be running").isFalse();
        assertThat(leaked).as("Failed startups should not leave files open").isLessThan(5);
    }

    @Test
    void shouldStartLaterWhenStartupFails() throws IOException {
        int port = findFreePort();
        long openBefore = countOpenFiles();

        try {
            // Fail to start while another socket holds the port
            try (ServerSocket _ = new ServerSocket(port)) {
                assertThatThrownBy(() -> server.listen(port)).as("Should fail while the port is taken").isInstanceOf(IllegalStateException.class);
            }

            long openAfter = countOpenFiles();

            // Assert the failed startup released its event loops, where open files can be counted
            assertThat(server.isRunning()).as("Server should not be running").isFalse();
            assumingThat(
                openBefore >= 0,
                () -> assertThat(openAfter - openBefore)
                    .as("Failed startup should not leave files open")
                    .isLessThan(5)
            );

            // Assert the server starts once the port is free again
            server.listen(port);
            assertThat(server.isRunning()).as("Server should be running").isTrue();
        } finally {
            stopIfNeeded(server);
        }
    }

    @Test
    void shouldStillReleaseResourcesOnStopWhenServerChannelClosesUnexpectedly() throws Exception {
        int port = findFreePort();
        long openBefore = countOpenFiles();

        EventLoopGroup bossGroup;
        EventLoopGroup workerGroup;

        try {
            server.listen(port);

            bossGroup = getEventLoopGroup(server, "bossGroup");
            workerGroup = getEventLoopGroup(server, "workerGroup");

            // Close the server channel without stop(), as if it failed
            Channel serverChannel = getServerChannel(server);
            CountDownLatch closed = new CountDownLatch(1);

            serverChannel.closeFuture().addListener(_ -> closed.countDown());
            serverChannel.close();

            assertThat(closed.await(5, TimeUnit.SECONDS)).as("Server channel should be closed").isTrue();

            // Assert the server is no longer accepting connections
            assertThat(server.isRunning()).as("Server should not be running").isFalse();
            assertThat(server.getLocalAddress()).as("Address after channel close").isNull();
            assertThatThrownBy(() -> server.listen(port))
                .as("Restart should require stop() first")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("call stop()");

            // stop() must still clean up event loop resources
            assertThatCode(server::stop).as("Stop should release the remaining resources").doesNotThrowAnyException();

            assertThat(bossGroup.isTerminated()).as("Boss event loop group should be terminated").isTrue();
            assertThat(workerGroup.isTerminated()).as("Worker event loop group should be terminated").isTrue();

            long openAfter = countOpenFiles();

            assumingThat(
                openBefore >= 0,
                () -> assertThat(openAfter - openBefore)
                    .as("Stop should not leave files open")
                    .isLessThan(5)
            );

            // Assert the server can be started again
            server.listen(port);
            assertThat(server.isRunning()).as("Server should be running").isTrue();
        } finally {
            stopIfNeeded(server);
        }
    }

    @Test
    void shouldThrowExceptionWhenListenIsCalledWhileStopping() throws Exception {
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);

        server.onMessage((_, _) -> {
            handlerEntered.countDown();

            try {
                releaseHandler.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        });

        int port = findFreePort();
        WebSocket client = null;
        FutureTask<Void> stopper = null;

        try {
            server.listen(port);

            // Keep a worker event loop busy in the message handler,
            // so stop() cannot complete immediately
            client = HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(
                    URI.create("ws://localhost:" + port + "/"),
                    new WebSocket.Listener() {
                    }
                )
                .join();

            client.sendText("block", true).join();

            assertThat(handlerEntered.await(5, TimeUnit.SECONDS)).as("Message handler should be called").isTrue();

            stopper = new FutureTask<>(() -> {
                server.stop();
                return null;
            });

            Thread.ofPlatform().start(stopper);

            awaitCondition(
                () -> !server.isRunning(),
                "Server should start stopping"
            );

            // Assert a new startup is rejected while the server is still stopping
            assertThatThrownBy(() -> server.listen(port))
                .as("Should not start while stopping")
                .isInstanceOf(IllegalStateException.class)
                .as("Should be rejected because the server is stopping")
                .hasMessageContaining("stopping");

            // Allow stop() to finish
            releaseHandler.countDown();

            // get() propagates exceptions thrown by stop()
            stopper.get(15, TimeUnit.SECONDS);

            // Assert the server can be started again once stopping completed
            server.listen(port);
            assertThat(server.isRunning()).as("Server should be running").isTrue();
        } finally {
            releaseHandler.countDown();

            if (client != null) {
                client.abort();
            }

            // If stop() was started but an assertion failed before get(),
            // still wait for it after unblocking the handler.
            if (stopper != null && !stopper.isDone()) {
                try {
                    stopper.get(15, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    // The main test path verifies stop() failures.
                    // This branch exists only to avoid leaking the test thread.
                }
            }

            stopIfNeeded(server);
        }
    }

    @Test
    void shouldThrowExceptionWhenConfigureIsCalledWhileStopping() throws Exception {
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);

        server.onMessage((_, _) -> {
            handlerEntered.countDown();

            try {
                releaseHandler.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        });

        int port = findFreePort();
        WebSocket client = null;
        FutureTask<Void> stopper = null;

        try {
            server.listen(port);

            // Keep a worker event loop busy in the message handler,
            // so stop() cannot complete immediately
            client = HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(
                    URI.create("ws://localhost:" + port + "/"),
                    new WebSocket.Listener() {
                    }
                )
                .join();

            client.sendText("block", true).join();

            assertThat(handlerEntered.await(5, TimeUnit.SECONDS)).as("Message handler should be called").isTrue();

            stopper = new FutureTask<>(() -> {
                server.stop();
                return null;
            });

            Thread.ofPlatform().start(stopper);

            awaitCondition(
                () -> !server.isRunning(),
                "Server should start stopping"
            );

            // Assert the configuration cannot be changed while the server is still stopping
            assertThatThrownBy(() -> server.configure(configurer -> configurer.setRespondWithBinaryFrame(true)))
                .as("Should not be reconfigured while stopping")
                .isInstanceOf(IllegalStateException.class);

            assertThat(server.getConfiguration().isRespondWithBinaryFrame()).as("Configuration should not change").isFalse();

            // Allow stop() to finish
            releaseHandler.countDown();

            // get() propagates exceptions thrown by stop()
            stopper.get(15, TimeUnit.SECONDS);
        } finally {
            releaseHandler.countDown();

            if (client != null) {
                client.abort();
            }

            // If stop() was started but an assertion failed before get(),
            // still wait for it after unblocking the handler.
            if (stopper != null && !stopper.isDone()) {
                try {
                    stopper.get(15, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    // The main test path verifies stop() failures.
                    // This branch exists only to avoid leaking the test thread.
                }
            }

            stopIfNeeded(server);
        }
    }

    private static EventLoopGroup getEventLoopGroup(
        WebSocketServer<?, ?> server,
        String fieldName
    ) throws ReflectiveOperationException {
        Field field = WebSocketServer.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (EventLoopGroup) field.get(server);
    }

    private static void stopIfNeeded(WebSocketServer<?, ?> server) {
        try {
            server.stop();
        } catch (IllegalStateException ignored) {
            // Already stopped, used only for test cleanup.
        }
    }

    private URI serverUri() {
        return URI.create("ws://127.0.0.1:" + server.getLocalAddress().getPort() + "/");
    }

    private static final class ClientListener implements WebSocket.Listener {

        private final CompletableFuture<Boolean> opened = new CompletableFuture<>();
        private final CompletableFuture<String> message = new CompletableFuture<>();
        private final CompletableFuture<Integer> closeCode = new CompletableFuture<>();
        private final StringBuilder text = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(1);
            opened.complete(true);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            text.append(data);
            if (last) {
                message.complete(text.toString());
            }
            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            closeCode.complete(statusCode);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            opened.completeExceptionally(error);
            message.completeExceptionally(error);
            closeCode.completeExceptionally(error);
        }
    }

    /**
     * Returns the server channel, to simulate it closing without stop().
     */
    private static Channel getServerChannel(WebSocketServer<?, ?> server) throws ReflectiveOperationException {
        Field field = WebSocketServer.class.getDeclaredField("serverChannel");
        field.setAccessible(true);
        return (Channel) field.get(server);
    }

    private static void awaitCondition(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);

        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime()).as(message).isLessThan(deadline);
            Thread.sleep(10);
        }
    }

    /**
     * Counts the files opened by this process, or returns -1 where /proc is not available.
     */
    private static long countOpenFiles() throws IOException {
        Path openFiles = Path.of("/proc/self/fd");
        return Files.isDirectory(openFiles) ? countFiles(openFiles) : -1;
    }

    private static long countFiles(Path directory) throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.count();
        }
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    void shouldBroadcastFromCloseHandlerWhileStopping() {
        List<Throwable> errors = new ArrayList<>();
        server
            .onClose((_, _, _) -> server.broadcast("bye"))
            .onError((_, exception) -> errors.add(exception))
            .listen(0);
        EmbeddedChannel first = connectWithUniqueId();
        EmbeddedChannel second = connectWithUniqueId();

        assertThatCode(server::stop).as("Stop should not fail").doesNotThrowAnyException();
        assertThat(errors).as("Broadcast from onClose should not fail").isEmpty();
        assertThat(first.isOpen() || second.isOpen()).as("Sessions should be closed").isFalse();
    }

    @Test
    void shouldNotOpenSessionsWhileStopping() {
        List<WebSocketSession<String, Object>> opened = new ArrayList<>();
        AtomicReference<Runnable> whileStopping = new AtomicReference<>(() -> {});
        server
            .onOpen(opened::add)
            .onClose((_, _, _) -> whileStopping.getAndSet(() -> {}).run())
            .listen(0);
        connectWithUniqueId();

        // A handshake whose 101 response is still being written when stop() starts
        EmbeddedChannel late = Util.newEmbeddedChannel(DefaultChannelId.newInstance(), new WebSocketServerChannelInitializer<>(server));
        List<Object> lateOutbound = new ArrayList<>();
        late.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void flush(ChannelHandlerContext context) {
                if (whileStopping.get() == null) {
                    context.flush();
                }
            }
        });
        Util.performHandshake(late, "/");
        EmbeddedChannel rejected = Util.newEmbeddedChannel(DefaultChannelId.newInstance(), new WebSocketServerChannelInitializer<>(server));

        whileStopping.set(() -> {
            whileStopping.set(null);
            late.flush();
            Util.performHandshake(rejected, "/");
        });
        server.stop();

        assertThat(opened).as("Only the session opened before stop() should be opened").hasSize(1);
        assertThat(late.isOpen()).as("Late session should be closed").isFalse();
        ByteBuf response = rejected.readOutbound();
        assertThat(response.toString(StandardCharsets.US_ASCII)).as("New handshake should be rejected").startsWith("HTTP/1.1 503");
        response.release();
    }

    private EmbeddedChannel connectWithUniqueId() {
        EmbeddedChannel channel = Util.newEmbeddedChannel(DefaultChannelId.newInstance(), new WebSocketServerChannelInitializer<>(server));
        Util.performHandshake(channel, "/");
        channel.releaseOutbound();
        return channel;
    }
}
