package pl.mbaracz.pongwire;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.mbaracz.pongwire.configuration.BackpressurePolicy;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class SendMessageAsyncTest {

    private static final int SENDER_THREADS = 12;
    private static final int MESSAGES_PER_SENDER = 100;

    private final List<WebSocketSession<String, Object>> opened = new ArrayList<>();
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            )
            .onOpen(opened::add);
    }

    @Test
    void shouldCompleteSendMessageAsyncSuccessfullyWhenMessageIsWritten() {
        EmbeddedChannel channel = Util.connect(server);

        CompletableFuture<Void> result = opened.getFirst().sendMessageAsync("Hello").toCompletableFuture();

        // Assert stage completed normally and the message was written
        assertThat(result.isDone()).as("Stage should be completed").isTrue();
        assertThat(result.isCompletedExceptionally()).as("Stage should complete normally").isFalse();

        TextWebSocketFrame frame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(TextWebSocketFrame.class)).actual();
        assertThat(frame.text()).as("Message should be written").isEqualTo("Hello");
        frame.release();
    }

    @Test
    @Timeout(20)
    void shouldDeliverEveryMessageWhenOneSessionIsUsedConcurrently() throws Exception {
        AtomicReference<WebSocketSession<String, Object>> openedSession = new AtomicReference<>();
        CountDownLatch sessionOpened = new CountDownLatch(1);
        CountDownLatch messagesReceived = new CountDownLatch(SENDER_THREADS * MESSAGES_PER_SENDER);
        ConcurrentLinkedQueue<String> outboundMessages = new ConcurrentLinkedQueue<>();
        ExecutorService senders = Executors.newFixedThreadPool(SENDER_THREADS);
        ExecutorService clientExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch ready = new CountDownLatch(SENDER_THREADS);
        CountDownLatch start = new CountDownLatch(1);
        CompletableFuture<?>[] sendFutures = new CompletableFuture<?>[SENDER_THREADS * MESSAGES_PER_SENDER];
        List<Future<?>> senderTasks = new ArrayList<>(SENDER_THREADS);
        List<String> expectedMessages = new ArrayList<>(SENDER_THREADS * MESSAGES_PER_SENDER);
        WebSocket client = null;

        try {
            server.onOpen(session -> {
                openedSession.set(session);
                sessionOpened.countDown();
            });
            int port = findFreePort();
            server.listen(port);

            client = HttpClient.newBuilder()
                .executor(clientExecutor)
                .build()
                .newWebSocketBuilder()
                .buildAsync(URI.create("ws://127.0.0.1:" + port), new WebSocket.Listener() {
                    private final StringBuilder message = new StringBuilder();

                    @Override
                    public void onOpen(WebSocket webSocket) {
                        webSocket.request(Long.MAX_VALUE);
                    }

                    @Override
                    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                        message.append(data);

                        if (last) {
                            outboundMessages.add(message.toString());
                            message.setLength(0);
                            messagesReceived.countDown();
                        }

                        return null;
                    }
                })
                .get(5, TimeUnit.SECONDS);

            assertThat(sessionOpened.await(5, TimeUnit.SECONDS)).as("The session should open").isTrue();
            WebSocketSession<String, Object> session = openedSession.get();

            for (int sender = 0; sender < SENDER_THREADS; sender++) {
                int senderId = sender;

                for (int sequence = 0; sequence < MESSAGES_PER_SENDER; sequence++) {
                    expectedMessages.add(message(senderId, sequence));
                }

                senderTasks.add(senders.submit(() -> {
                    ready.countDown();
                    start.await();

                    for (int sequence = 0; sequence < MESSAGES_PER_SENDER; sequence++) {
                        int index = senderId * MESSAGES_PER_SENDER + sequence;
                        sendFutures[index] = session.sendMessageAsync(message(senderId, sequence)).toCompletableFuture();
                    }

                    return null;
                }));
            }

            assertThat(ready.await(5, TimeUnit.SECONDS)).as("Every sender should be ready").isTrue();
            start.countDown();

            for (Future<?> senderTask : senderTasks) {
                senderTask.get(5, TimeUnit.SECONDS);
            }

            CompletableFuture.allOf(sendFutures).get(5, TimeUnit.SECONDS);
            assertThat(messagesReceived.await(5, TimeUnit.SECONDS)).as("Every message should reach the client").isTrue();

            assertThat(sendFutures)
                .allMatch(CompletableFuture::isDone)
                .noneMatch(CompletableFuture::isCompletedExceptionally);

            // Preserve the observed global outbound order, but do not make it part of the contract:
            // concurrent callers have no single, well-defined invocation order to compare it with.
            assertThat(outboundMessages).containsExactlyInAnyOrderElementsOf(expectedMessages);
        } finally {
            start.countDown();
            if (client != null) {
                client.abort();
            }
            if (server.isRunning()) {
                server.stop();
            }
            senders.shutdownNow();
            clientExecutor.shutdownNow();
            assertThat(senders.awaitTermination(5, TimeUnit.SECONDS)).as("Sender threads should stop").isTrue();
            assertThat(clientExecutor.awaitTermination(5, TimeUnit.SECONDS)).as("Client thread should stop").isTrue();
        }
    }

    @Test
    @Timeout(20)
    void shouldOrderClientCloseBeforeOffEventLoopSendAndServerClose() throws Exception {
        AtomicReference<WebSocketSession<String, Object>> openedSession = new AtomicReference<>();
        CountDownLatch sessionOpened = new CountDownLatch(1);
        CountDownLatch eventLoopBlocked = new CountDownLatch(1);
        CountDownLatch releaseEventLoop = new CountDownLatch(1);
        CompletableFuture<Integer> clientCloseCode = new CompletableFuture<>();
        CompletableFuture<Throwable> inboundFailure = new CompletableFuture<>();
        ConcurrentLinkedQueue<String> received = new ConcurrentLinkedQueue<>();
        ExecutorService clientExecutor = Executors.newSingleThreadExecutor();
        WebSocket client = null;

        try {
            server.onOpen(session -> {
                openedSession.set(session);
                sessionOpened.countDown();
            });
            int port = findFreePort();
            server.listen(port);
            client = HttpClient.newBuilder()
                .executor(clientExecutor)
                .build()
                .newWebSocketBuilder()
                .buildAsync(URI.create("ws://127.0.0.1:" + port), new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                        webSocket.request(Long.MAX_VALUE);
                    }

                    @Override
                    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                        received.add(data.toString());
                        return null;
                    }

                    @Override
                    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                        clientCloseCode.complete(statusCode);
                        return null;
                    }
                })
                .get(5, TimeUnit.SECONDS);

            assertThat(sessionOpened.await(5, TimeUnit.SECONDS)).isTrue();
            WebSocketSession<String, Object> session = openedSession.get();
            var channel = session.getChannelContext().channel();

            channel.eventLoop().execute(() -> {
                eventLoopBlocked.countDown();
                try {
                    releaseEventLoop.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(eventLoopBlocked.await(5, TimeUnit.SECONDS)).isTrue();

            ChannelHandlerContext handlerContext = channel.pipeline().context(WebSocketServerHandler.class);
            channel.eventLoop().execute(() -> {
                try {
                    ((WebSocketServerHandler<?, ?>) handlerContext.handler())
                        .channelRead(handlerContext, new CloseWebSocketFrame(1000, "client"));
                } catch (Exception exception) {
                    inboundFailure.complete(exception);
                }
            });
            CompletableFuture<Void> send = session.sendMessageAsync("late").toCompletableFuture();
            CompletableFuture<Void> serverClose = session.close(1001, "server").toCompletableFuture();

            assertThat(send).as("Off-event-loop send waits for its queued task").isNotDone();
            assertThat(ClosingHandshake.isClosing(channel))
                .as("Queued server Close has not changed the state yet").isFalse();

            releaseEventLoop.countDown();

            assertThatThrownBy(() -> send.get(5, TimeUnit.SECONDS))
                .cause().isInstanceOf(IllegalStateException.class);
            assertThat(clientCloseCode.get(5, TimeUnit.SECONDS)).isEqualTo(1000);
            assertThat(received).as("No data frame follows Close").isEmpty();
            assertThat(inboundFailure).as("Inbound Close handling succeeded").isNotDone();
            serverClose.get(5, TimeUnit.SECONDS);
        } finally {
            releaseEventLoop.countDown();
            if (client != null) {
                client.abort();
            }
            if (server.isRunning()) {
                server.stop();
            }
            clientExecutor.shutdownNow();
            assertThat(clientExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @Timeout(20)
    void shouldCompleteStagesWhenSessionIsUsedAfterServerStopped() throws Exception {
        CountDownLatch sessionOpened = new CountDownLatch(1);
        ExecutorService clientExecutor = Executors.newSingleThreadExecutor();
        WebSocket client = null;

        try {
            server
                .onOpen(session -> {
                    opened.add(session);
                    sessionOpened.countDown();
                })
                .listen(0);
            client = HttpClient.newBuilder()
                .executor(clientExecutor)
                .build()
                .newWebSocketBuilder()
                .buildAsync(URI.create("ws://127.0.0.1:" + server.getLocalAddress().getPort()), new WebSocket.Listener() {
                })
                .get(5, TimeUnit.SECONDS);
            assertThat(sessionOpened.await(5, TimeUnit.SECONDS)).isTrue();

            // The event loops are terminated once stop() returns
            server.stop();
            WebSocketSession<String, Object> session = opened.getFirst();
            CompletableFuture<Void> send = session.sendMessageAsync("late").toCompletableFuture();
            CompletableFuture<Void> close = session.close(1000, "late").toCompletableFuture();

            assertThatThrownBy(() -> send.get(5, TimeUnit.SECONDS)).as("Send should fail").cause().isNotNull();
            assertThat(close).as("Close should complete").succeedsWithin(5, TimeUnit.SECONDS);
        } finally {
            if (client != null) {
                client.abort();
            }
            clientExecutor.shutdownNow();
            assertThat(clientExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void shouldCompleteSendMessageAsyncExceptionallyWhenWriteFails() {
        EmbeddedChannel channel = Util.connect(server);

        // Close the connection before sending
        channel.close();
        CompletableFuture<Void> result = opened.getFirst().sendMessageAsync("Hello").toCompletableFuture();

        // Assert stage failed with the cause of the failed write
        assertThat(result.isCompletedExceptionally()).as("Stage should complete exceptionally").isTrue();

        assertThatThrownBy(result::join)
            .isInstanceOf(CompletionException.class)
            .as("Should fail because the channel is closed")
            .cause()
            .isInstanceOf(ClosedChannelException.class);
    }

    @Test
    void shouldCompleteSendMessageAsyncExceptionallyWhenEncodingFails() {
        IllegalStateException encoderFailure = new IllegalStateException("Encoding failed");
        server.configure(configurer -> configurer.setMessageEncoder(_ -> {
            throw encoderFailure;
        }));

        EmbeddedChannel channel = Util.connect(server);

        // Encoder failures are reported through the stage instead of being thrown
        CompletableFuture<Void> result = opened.getFirst().sendMessageAsync("Hello").toCompletableFuture();

        // Assert stage failed with the encoder exception and nothing was written
        assertThat(result.isCompletedExceptionally()).as("Stage should complete exceptionally").isTrue();

        assertThatThrownBy(result::join)
            .isInstanceOf(CompletionException.class)
            .as("Should fail with the encoder exception")
            .cause()
            .isSameAs(encoderFailure);
        assertThat(Util.readFromServer(channel)).as("Nothing should be written").isNull();
    }

    @Test
    void shouldRejectNewMessageWhenUnwritableUnderRejectNewPolicy() {
        server.configure(configurer -> configurer.setBackpressurePolicy(BackpressurePolicy.REJECT_NEW));
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = opened.getFirst();

        channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
        channel.runPendingTasks();
        CompletableFuture<Void> result = session.sendMessageAsync("rejected").toCompletableFuture();

        assertThatThrownBy(result::join)
            .isInstanceOf(CompletionException.class)
            .as("Should fail with a backpressure error")
            .cause()
            .isInstanceOf(BackpressureException.class);
        assertThat(Util.readFromServer(channel)).as("Rejected message should not be written").isNull();

        channel.close();
    }

    @Test
    void shouldSendAgainAfterWritabilityRecoversUnderRejectNewPolicy() {
        server.configure(configurer -> configurer.setBackpressurePolicy(BackpressurePolicy.REJECT_NEW));
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = opened.getFirst();

        channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
        channel.runPendingTasks();
        session.sendMessageAsync("rejected");
        channel.unsafe().outboundBuffer().setUserDefinedWritability(1, true);
        channel.runPendingTasks();

        CompletableFuture<Void> result = session.sendMessageAsync("accepted").toCompletableFuture();

        assertThat(result.isCompletedExceptionally()).as("Recovered send should succeed").isFalse();
        TextWebSocketFrame frame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(TextWebSocketFrame.class)).actual();
        assertThat(frame.text()).isEqualTo("accepted");
        frame.release();
    }

    @Test
    void shouldKeepAcceptingNewMessagesWhenUnwritableUnderDefaultBufferPolicy() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = opened.getFirst();

        channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
        channel.runPendingTasks();
        CompletableFuture<Void> result = session.sendMessageAsync("accepted").toCompletableFuture();

        assertThat(result.isCompletedExceptionally()).as("Default policy should preserve existing behavior").isFalse();
        TextWebSocketFrame frame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(TextWebSocketFrame.class)).actual();
        assertThat(frame.text()).isEqualTo("accepted");
        frame.release();
    }

    private static String message(int sender, int sequence) {
        return "sender-" + sender + "-message-" + sequence;
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
