package pl.mbaracz.pongwire.benchmark;

import com.sun.management.UnixOperatingSystemMXBean;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.util.concurrent.Future;
import pl.mbaracz.pongwire.WebSocketServer;
import pl.mbaracz.pongwire.WebSocketSession;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * WebSocket load benchmark using real TCP connections over loopback.
 * Runs as a standalone application and measures the server together
 * with the client and network stack.
 */
public final class WebSocketLoadBenchmark {

    private static final int BASE_FILE_DESCRIPTOR_HEADROOM = 128;
    private static final Duration HANDSHAKE_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration MESSAGE_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration CLIENT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);

    private final Options options;
    private final String payload;
    private final AtomicInteger connected = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();
    private final LongAdder received = new LongAdder();
    private final LongAdder messageErrors = new LongAdder();
    private final CompletableFuture<Void> handshakesFinished = new CompletableFuture<>();
    private final List<Channel> clients = new ArrayList<>();

    private WebSocketLoadBenchmark(Options options) {
        this.options = options;
        this.payload = "x".repeat(options.payloadSize());
    }

    static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

        try {
            Options options = Options.parse(args);
            if (options.help()) {
                printUsage();
                return;
            }

            boolean successful = new WebSocketLoadBenchmark(options).run();
            if (!successful) {
                System.exit(1);
            }
        } catch (IllegalArgumentException exception) {
            System.err.println("Error: " + exception.getMessage());
            System.err.println();
            printUsage();
            System.exit(2);
        } catch (Exception exception) {
            System.err.println("Load benchmark failed: " + exception.getMessage());
            exception.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private boolean run() throws Exception {
        checkFileDescriptorLimit(options.connections());

        WebSocketServer<String, Object> server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            )
            .onMessage(WebSocketSession::sendMessage);

        EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            server.listen(0);
            InetSocketAddress serverAddress = server.getLocalAddress();
            URI uri = new URI("ws", null, InetAddress.getLoopbackAddress().getHostAddress(),
                serverAddress.getPort(), "/", null, null);

            Bootstrap bootstrap = clientBootstrap(clientGroup, uri);
            long connectStarted = System.nanoTime();
            connectAll(bootstrap, serverAddress.getPort());
            await(handshakesFinished, HANDSHAKE_TIMEOUT, "handshakes");
            long connectNanos = System.nanoTime() - connectStarted;

            TimeUnit.SECONDS.sleep(options.holdSeconds());
            int liveAfterHold = countActiveClients();
            int serverSessionsAfterHold = server.getSessionCount();

            long expectedMessages = (long) connected.get() * options.messagesPerClient();
            long messageStarted = System.nanoTime();
            long sent = sendMessages();
            waitForMessages(sent, MESSAGE_TIMEOUT);
            long messageNanos = System.nanoTime() - messageStarted;

            long shutdownStarted = System.nanoTime();
            server.stop();
            long shutdownNanos = System.nanoTime() - shutdownStarted;
            int sessionsRemaining = server.getSessionCount();

            long receivedMessages = received.sum();
            long lostMessages = Math.max(0, sent - receivedMessages);
            printResults(
                connectNanos, liveAfterHold, serverSessionsAfterHold, sent,
                receivedMessages, lostMessages, messageNanos, shutdownNanos, sessionsRemaining
            );

            return connected.get() == options.connections()
                && failed.get() == 0
                && liveAfterHold == connected.get()
                && serverSessionsAfterHold == connected.get()
                && sent == expectedMessages
                && receivedMessages == sent
                && messageErrors.sum() == 0
                && sessionsRemaining == 0;
        } finally {
            if (server.isRunning()) {
                server.stop();
            }
            closeClients();
            Future<?> termination = clientGroup.shutdownGracefully(0, CLIENT_SHUTDOWN_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            termination.awaitUninterruptibly();
        }
    }

    private Bootstrap clientBootstrap(EventLoopGroup clientGroup, URI uri) {
        WebSocketClientProtocolConfig protocolConfig = WebSocketClientProtocolConfig.newBuilder()
            .webSocketUri(uri)
            .version(WebSocketVersion.V13)
            .allowExtensions(false)
            .handshakeTimeoutMillis(HANDSHAKE_TIMEOUT.toMillis())
            .build();

        return new Bootstrap()
            .group(clientGroup)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel channel) {
                    channel.pipeline()
                        .addLast(new HttpClientCodec())
                        .addLast(new HttpObjectAggregator(8192))
                        .addLast(new WebSocketClientProtocolHandler(protocolConfig))
                        .addLast(new LoadClientHandler());
                }
            });
    }

    private void connectAll(Bootstrap bootstrap, int port) {
        for (int i = 0; i < options.connections(); i++) {
            ChannelFuture connectFuture = bootstrap.connect(InetAddress.getLoopbackAddress(), port);

            synchronized (clients) {
                clients.add(connectFuture.channel());
            }

            connectFuture.addListener(future -> {
                if (!future.isSuccess()) {
                    connectFuture.channel().pipeline().get(LoadClientHandler.class).recordHandshakeFailureOnce();
                }
            });
        }
    }

    private long sendMessages() {
        long sent = 0;
        List<Channel> snapshot;

        synchronized (clients) {
            snapshot = List.copyOf(clients);
        }

        for (Channel client : snapshot) {
            if (!client.isActive()) {
                continue;
            }
            for (int i = 0; i < options.messagesPerClient(); i++) {
                sent++;
                client.write(new TextWebSocketFrame(payload)).addListener(future -> {
                    if (!future.isSuccess()) {
                        messageErrors.increment();
                    }
                });
            }
            client.flush();
        }
        return sent;
    }

    private void waitForMessages(long expected, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();

        while (received.sum() < expected && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
    }

    private int countActiveClients() {
        synchronized (clients) {
            return (int) clients.stream().filter(Channel::isActive).count();
        }
    }

    private void closeClients() {
        List<Channel> snapshot;

        synchronized (clients) {
            snapshot = List.copyOf(clients);
        }

        for (Channel client : snapshot) {
            client.close().awaitUninterruptibly();
        }
    }

    private void recordHandshakeSuccess() {
        connected.incrementAndGet();
        finishHandshakeAttempt();
    }

    private void recordHandshakeFailure() {
        failed.incrementAndGet();
        finishHandshakeAttempt();
    }

    private void finishHandshakeAttempt() {
        if (connected.get() + failed.get() == options.connections()) {
            handshakesFinished.complete(null);
        }
    }

    private void printResults(
        long connectNanos, int liveAfterHold, int serverSessionsAfterHold, long sent,
        long receivedMessages, long lostMessages, long messageNanos, long shutdownNanos,
        int sessionsRemaining
    ) {
        System.out.printf(Locale.ROOT, "%nConnections:          %d%n", options.connections());
        System.out.printf(Locale.ROOT, "Connected:            %d%n", connected.get());
        System.out.printf(Locale.ROOT, "Failed:               %d%n", failed.get());
        System.out.printf(Locale.ROOT, "Connect time:         %.2f s%n", seconds(connectNanos));
        System.out.printf(Locale.ROOT, "Connect rate:         %.0f conn/s%n", rate(connected.get(), connectNanos));
        System.out.printf(Locale.ROOT, "%-22s%d%n", "Live after " + options.holdSeconds() + " s:", liveAfterHold);
        System.out.printf(Locale.ROOT, "Server sessions:      %d%n%n", serverSessionsAfterHold);
        System.out.printf(Locale.ROOT, "Messages sent:        %d%n", sent);
        System.out.printf(Locale.ROOT, "Messages received:    %d%n", receivedMessages);
        System.out.printf(Locale.ROOT, "Messages lost:        %d%n", lostMessages);
        System.out.printf(Locale.ROOT, "Message errors:       %d%n", messageErrors.sum());
        System.out.printf(Locale.ROOT, "Message throughput:   %.0f msg/s%n%n", rate(receivedMessages, messageNanos));
        System.out.printf(Locale.ROOT, "Graceful shutdown:    %.0f ms%n", shutdownNanos / 1_000_000.0);
        System.out.printf(Locale.ROOT, "Sessions remaining:   %d%n", sessionsRemaining);
    }

    private static double seconds(long nanos) {
        return nanos / 1_000_000_000.0;
    }

    private static double rate(long count, long nanos) {
        return nanos == 0 ? 0 : count / seconds(nanos);
    }

    private static void await(CompletableFuture<Void> future, Duration timeout, String operation) throws Exception {
        try {
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException exception) {
            throw new IllegalStateException("Timed out waiting for " + operation, exception);
        }
    }

    private static void checkFileDescriptorLimit(int connections) {
        if (!(ManagementFactory.getOperatingSystemMXBean() instanceof UnixOperatingSystemMXBean osBean)) {
            System.err.println("Warning: cannot check the file descriptor limit on this operating system.");
            return;
        }

        long open = osBean.getOpenFileDescriptorCount();
        long limit = osBean.getMaxFileDescriptorCount();
        long eventLoopHeadroom = 4L * Runtime.getRuntime().availableProcessors() + BASE_FILE_DESCRIPTOR_HEADROOM;
        long required = open + 2L * connections + eventLoopHeadroom;

        if (limit < required) {
            throw new IllegalArgumentException("file descriptor limit is too low: " + limit + " available limit, "
                + open + " currently open, about " + required + " required for " + connections
                + " loopback connections. Raise the limit (for example: ulimit -n " + required + ") and retry."
            );
        }

        System.out.printf(
            Locale.ROOT,
            "File descriptors:     %d open / %d limit (estimated requirement: %d)%n",
            open, limit, required
        );
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar pongwire-load.jar [options]");
        System.out.println();
        System.out.println("  --connections N             number of concurrent clients (default: 10000)");
        System.out.println("  --messages-per-client N     messages sent by every client (default: 10)");
        System.out.println("  --payload-size N            text payload size in ASCII bytes (default: 128)");
        System.out.println("  --hold-seconds N            seconds to keep all connections idle (default: 10)");
        System.out.println("  --help                      show this help");
    }

    private final class LoadClientHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

        private final AtomicBoolean handshakeRecorded = new AtomicBoolean();

        @Override
        public void userEventTriggered(ChannelHandlerContext context, Object event) throws Exception {
            if (event == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE
                && handshakeRecorded.compareAndSet(false, true)) {
                recordHandshakeSuccess();
            }
            super.userEventTriggered(context, event);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext context, TextWebSocketFrame frame) {
            if (frame.text().equals(payload)) {
                received.increment();
            } else {
                messageErrors.increment();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) throws Exception {
            recordHandshakeFailureOnce();
            super.channelInactive(context);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
            if (!recordHandshakeFailureOnce()) {
                messageErrors.increment();
            }
            context.close();
        }

        private boolean recordHandshakeFailureOnce() {
            if (handshakeRecorded.compareAndSet(false, true)) {
                recordHandshakeFailure();
                return true;
            }
            return false;
        }
    }

    private record Options(int connections, int messagesPerClient, int payloadSize, int holdSeconds, boolean help) {

        private static Options parse(String[] args) {
            int connections = 10_000;
            int messagesPerClient = 10;
            int payloadSize = 128;
            int holdSeconds = 10;
            boolean help = false;

            for (int i = 0; i < args.length; i++) {
                String argument = args[i];
                switch (argument) {
                    case "--connections" -> connections = positiveInt(value(args, ++i, argument), argument);
                    case "--messages-per-client" ->
                        messagesPerClient = positiveInt(value(args, ++i, argument), argument);
                    case "--payload-size" -> payloadSize = positiveInt(value(args, ++i, argument), argument);
                    case "--hold-seconds" -> holdSeconds = nonNegativeInt(value(args, ++i, argument), argument);
                    case "--help", "-h" -> help = true;
                    default -> throw new IllegalArgumentException("unknown option: " + argument);
                }
            }
            if ((long) connections * messagesPerClient > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("connections * messages-per-client must not exceed " + Integer.MAX_VALUE);
            }
            return new Options(connections, messagesPerClient, payloadSize, holdSeconds, help);
        }

        private static String value(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException("missing value for " + option);
            }
            return args[index];
        }

        private static int positiveInt(String value, String option) {
            int parsed = nonNegativeInt(value, option);

            if (parsed == 0) {
                throw new IllegalArgumentException(option + " must be greater than zero");
            }
            return parsed;
        }

        private static int nonNegativeInt(String value, String option) {
            try {
                int parsed = Integer.parseInt(value);

                if (parsed < 0) {
                    throw new IllegalArgumentException(option + " must not be negative");
                }
                return parsed;
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(option + " must be an integer: " + value, exception);
            }
        }
    }
}
