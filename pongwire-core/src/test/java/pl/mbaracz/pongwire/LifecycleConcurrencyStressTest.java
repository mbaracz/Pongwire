package pl.mbaracz.pongwire;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class LifecycleConcurrencyStressTest {

    private static final int CHANNELS = 128;
    private static final int WORKER_THREADS = 16;
    private static final int CALLBACK_THREADS = 8;
    private static final int MESSAGES_PER_SESSION = 4;
    private static final int PUB_SUB_ITERATIONS = 32;
    private static final int SLOW_CHANNELS = 8;
    private static final String HELD_FLUSH_HANDLER = "held-flush";

    private record Connection(EmbeddedChannel channel, WebSocketSession<String, Object> session) {
    }

    private static final class HoldFlushes extends ChannelOutboundHandlerAdapter {
        @Override
        public void flush(ChannelHandlerContext context) {
        }
    }

    @Test
    @Timeout(30)
    void shouldKeepWholeSessionLifecycleConsistentUnderConcurrentLoad() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(WORKER_THREADS);
        ExecutorService callbackExecutor = Executors.newFixedThreadPool(CALLBACK_THREADS);
        ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CompletableFuture<Void>> sendFutures = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CompletableFuture<Void>> closeFutures = new ConcurrentLinkedQueue<>();
        Map<WebSocketSession<String, Object>, AtomicInteger> closeCallbacks = new ConcurrentHashMap<>();
        Map<WebSocketSession<String, Object>, List<Boolean>> writabilityChanges = new ConcurrentHashMap<>();
        AtomicInteger receivedMessages = new AtomicInteger();
        CountDownLatch opened = new CountDownLatch(CHANNELS);
        CountDownLatch closed = new CountDownLatch(CHANNELS);

        WebSocketServer<String, Object> server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                .setCallbackExecutor(callbackExecutor)
                .setCloseTimeout(Duration.ofMillis(100))
                .setWriteBufferWaterMark(1024, 2048)
            )
            .onOpen(_ -> opened.countDown())
            .onMessage((_, _) -> receivedMessages.incrementAndGet())
            .onWritabilityChanged((session, writable) -> writabilityChanges
                .computeIfAbsent(session, _ -> new ArrayList<>())
                .add(writable)
            )
            .onClose((session, _, _) -> {
                closeCallbacks.computeIfAbsent(session, _ -> new AtomicInteger()).incrementAndGet();
                closed.countDown();
            })
            .observer(new WebSocketServerObserver<>() {
                @Override
                public void exception(WebSocketSession<String, Object> session, Throwable exception) {
                    exceptions.add(exception);
                }
            });

        List<Connection> connections = new ArrayList<>();

        try {
            server.listen(0);

            List<Future<Connection>> openingTasks = submitTogether(workers, CHANNELS, _ -> {
                EmbeddedChannel channel = connect(server);
                WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
                return new Connection(channel, session);
            });
            connections.addAll(await(openingTasks));

            assertThat(opened.await(10, TimeUnit.SECONDS)).as("Every open callback should finish").isTrue();
            assertThat(connections).allMatch(connection -> connection.session() != null);
            assertThat(server.getConnectedSessions()).hasSize(CHANNELS);

            for (int index = 0; index < SLOW_CHANNELS; index++) {
                connections.get(index).channel().pipeline().addFirst(HELD_FLUSH_HANDLER, new HoldFlushes());
            }

            List<Future<Void>> subscriptionTasks = submitTogether(workers, CHANNELS, index -> {
                Connection connection = connections.get(index);
                server.subscribe(connection.session(), "shared-" + index % WORKER_THREADS);
                return null;
            });
            await(subscriptionTasks);

            List<Future<Void>> trafficTasks = submitTogether(workers, CHANNELS, index -> {
                Connection connection = connections.get(index);

                for (int message = 0; message < MESSAGES_PER_SESSION; message++) {
                    Util.sendFromClient(connection.channel(), new TextWebSocketFrame("client-" + message));
                    sendFutures.add(connection.session()
                        .sendMessageAsync("server-" + "x".repeat(4096))
                        .toCompletableFuture());
                }

                return null;
            });
            await(trafficTasks);

            // One task publishes to each shared topic while the other tasks repeatedly leave and rejoin it.
            // This keeps a single writer per EmbeddedChannel, matching real Netty event-loop serialization.
            List<Future<Void>> pubSubTasks = submitTogether(workers, CHANNELS, index -> {
                if (index < WORKER_THREADS) {
                    String topic = "shared-" + index;

                    for (int iteration = 0; iteration < PUB_SUB_ITERATIONS; iteration++) {
                        server.publish(topic, "published-" + iteration);
                    }
                } else {
                    WebSocketSession<String, Object> session = connections.get(index).session();
                    String topic = "shared-" + index % WORKER_THREADS;

                    for (int iteration = 0; iteration < PUB_SUB_ITERATIONS; iteration++) {
                        server.unsubscribe(session, topic);
                        server.subscribe(session, topic);
                    }
                }

                return null;
            });
            await(pubSubTasks);

            List<Future<Void>> drainTasks = submitTogether(workers, CHANNELS, index -> {
                EmbeddedChannel channel = connections.get(index).channel();
                channel.runPendingTasks();

                if (index < SLOW_CHANNELS) {
                    assertThat(channel.isWritable()).as("Slow channel should cross its high watermark").isFalse();
                    channel.pipeline().remove(HELD_FLUSH_HANDLER);
                    channel.flush();
                    assertThat(channel.isWritable()).as("Drained channel should cross its low watermark").isTrue();
                }

                channel.releaseOutbound();
                return null;
            });
            await(drainTasks);

            CompletableFuture<Void> allSends = CompletableFuture.allOf(sendFutures.toArray(CompletableFuture[]::new));
            allSends.get(10, TimeUnit.SECONDS);

            List<Future<Void>> closingTasks = submitTogether(workers, CHANNELS, index -> {
                Connection connection = connections.get(index);

                if (index % 3 == 0) {
                    Util.sendFromClient(connection.channel(), new CloseWebSocketFrame(
                        WebSocketCloseStatus.NORMAL_CLOSURE,
                        "client"
                    ));
                } else if (index % 3 == 1) {
                    CompletableFuture<Void> closeFuture = connection.session()
                        .close(WebSocketCloseStatus.NORMAL_CLOSURE.code(), "server")
                        .toCompletableFuture();
                    closeFutures.add(closeFuture);

                    WebSocketFrame closeFrame = Util.readFromServer(connection.channel());
                    assertThat(closeFrame).asInstanceOf(type(CloseWebSocketFrame.class));
                    closeFrame.release();
                    Util.sendFromClient(connection.channel(), new CloseWebSocketFrame(WebSocketCloseStatus.NORMAL_CLOSURE));
                }

                return null;
            });
            await(closingTasks);

            server.stop();

            CompletableFuture<Void> allCloses = CompletableFuture.allOf(closeFutures.toArray(CompletableFuture[]::new));
            allCloses.get(10, TimeUnit.SECONDS);
            assertThat(closed.await(10, TimeUnit.SECONDS)).as("Every close callback should finish").isTrue();

            callbackExecutor.shutdown();
            assertThat(callbackExecutor.awaitTermination(10, TimeUnit.SECONDS))
                .as("Callback executor should drain")
                .isTrue();

            assertThat(openingTasks).allMatch(Future::isDone);
            assertThat(subscriptionTasks).allMatch(Future::isDone);
            assertThat(trafficTasks).allMatch(Future::isDone);
            assertThat(pubSubTasks).allMatch(Future::isDone);
            assertThat(drainTasks).allMatch(Future::isDone);
            assertThat(closingTasks).allMatch(Future::isDone);
            assertThat(sendFutures)
                .allMatch(CompletableFuture::isDone)
                .noneMatch(CompletableFuture::isCompletedExceptionally);
            assertThat(closeFutures)
                .allMatch(CompletableFuture::isDone)
                .noneMatch(CompletableFuture::isCompletedExceptionally);
            assertThat(connections).allMatch(connection -> connection.channel().closeFuture().isDone());
            assertThat(exceptions).as("No exception should reach the pipeline observer").isEmpty();
            assertThat(receivedMessages).hasValue(CHANNELS * MESSAGES_PER_SESSION);
            assertThat(closeCallbacks).hasSize(CHANNELS);
            assertThat(closeCallbacks.values()).allMatch(count -> count.get() == 1);
            assertThat(connections.subList(0, SLOW_CHANNELS))
                .allMatch(connection -> writabilityChanges.containsKey(connection.session()));
            assertThat(writabilityChanges.values()).allSatisfy(changes -> assertThat(changes).contains(false, true));
            assertThat(server.getConnectedSessions()).isEmpty();
            assertThat(server.getTopics()).isEmpty();
        } finally {
            if (server.isRunning()) {
                server.stop();
            }
            connections.forEach(connection -> connection.channel().finishAndReleaseAll());
            workers.shutdownNow();
            callbackExecutor.shutdownNow();
        }
    }

    private static <T> List<Future<T>> submitTogether(
        ExecutorService executor,
        int tasks,
        IntFunction<T> operation
    ) {
        CountDownLatch start = new CountDownLatch(1);
        List<Future<T>> futures = new ArrayList<>(tasks);

        for (int index = 0; index < tasks; index++) {
            int task = index;
            futures.add(executor.submit(() -> {
                start.await();
                return operation.apply(task);
            }));
        }

        start.countDown();
        return futures;
    }

    private static EmbeddedChannel connect(WebSocketServer<String, Object> server) {
        EmbeddedChannel channel = Util.newEmbeddedChannel(
            DefaultChannelId.newInstance(),
            new WebSocketServerChannelInitializer<>(server)
        );
        Util.performHandshake(channel, "/");
        channel.releaseOutbound();
        return channel;
    }

    private static <T> List<T> await(List<? extends Future<T>> futures) throws Exception {
        List<T> results = new ArrayList<>(futures.size());

        for (Future<T> future : futures) {
            results.add(future.get(20, TimeUnit.SECONDS));
        }

        return results;
    }
}
