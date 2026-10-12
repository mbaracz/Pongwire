package pl.mbaracz.pongwire;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class CallbackExecutorTest {

    private ExecutorService executor;
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(4, Thread.ofPlatform().name("callback-", 0).factory());
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                .setCallbackExecutor(executor)
            );
    }

    @AfterEach
    void tearDown() {
        executor.close();
    }

    private static void send(EmbeddedChannel channel, String... messages) {
        for (String message : messages) {
            Util.sendFromClient(channel, new TextWebSocketFrame(message));
        }
    }

    /**
     * Waits for the latch at most the given time, without failing when it is not counted down.
     */
    private static void awaitQuietly(CountDownLatch latch, long millis) {
        try {
            latch.await(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void shouldRunCallbacksOnCallbackExecutorWhenItIsConfigured() throws InterruptedException {
        List<String> threads = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(3);

        server
            .onOpen(_ -> {
                threads.add(Thread.currentThread().getName());
                done.countDown();
            })
            .onMessage((_, _) -> {
                threads.add(Thread.currentThread().getName());
                done.countDown();
            })
            .onClose((_, _, _) -> {
                threads.add(Thread.currentThread().getName());
                done.countDown();
            });

        EmbeddedChannel channel = Util.connect(server);
        send(channel, "Hello");
        channel.close();

        assertThat(done.await(5, TimeUnit.SECONDS)).as("Callbacks should be called").isTrue();
        assertThat(threads).as("Callbacks should run on the executor").allMatch(name -> name.startsWith("callback-"));
    }

    @Test
    void shouldRunCallbacksInOrderWhenMessagesArrive() throws InterruptedException {
        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(100);

        server.onMessage((_, message) -> {
            received.add(message);
            done.countDown();
        });

        EmbeddedChannel channel = Util.connect(server);
        List<String> sent = IntStream.range(0, 100).mapToObj(String::valueOf).toList();

        send(channel, sent.toArray(String[]::new));

        assertThat(done.await(5, TimeUnit.SECONDS)).as("Callbacks should be called").isTrue();
        assertThat(received).as("Callbacks should run in the order of the messages").isEqualTo(sent);
    }

    @Test
    void shouldNotLetSecondCallbackOvertakeFirstWhenFirstIsSlow() throws InterruptedException {
        List<String> events = new CopyOnWriteArrayList<>();
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);

        server.onMessage((_, message) -> {
            events.add("start " + message);

            if (message.equals("second")) {
                secondStarted.countDown();
            } else {
                // Give the second callback the time to start, which it must not use
                awaitQuietly(secondStarted, 200);
            }

            events.add("end " + message);
            done.countDown();
        });

        EmbeddedChannel channel = Util.connect(server);
        send(channel, "first", "second");

        assertThat(done.await(5, TimeUnit.SECONDS)).as("Callbacks should be called").isTrue();
        assertThat(events)
            .as("Second callback should wait for the first one")
            .isEqualTo(List.of("start first", "end first", "start second", "end second"));
    }

    @Test
    void shouldStillRunLaterCallbacksWhenCallbackThrows() throws InterruptedException {
        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);

        server.onMessage((_, message) -> {
            if (message.equals("fail")) {
                throw new IllegalStateException("Callback failed");
            }

            received.add(message);
            done.countDown();
        });

        EmbeddedChannel channel = Util.connect(server);
        send(channel, "fail", "after");

        assertThat(done.await(5, TimeUnit.SECONDS)).as("Callback after the failed one should be called").isTrue();
        assertThat(received).as("Message after the failed callback should be received").isEqualTo(List.of("after"));
    }

    @Test
    void shouldRunCloseCallbackAfterQueuedMessagesWhenSessionCloses() throws InterruptedException {
        List<String> events = new CopyOnWriteArrayList<>();
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(3);

        server
            .onMessage((_, message) -> {
                if (message.equals("first")) {
                    // Give the close callback the time to start, which it must not use
                    awaitQuietly(closeStarted, 200);
                }

                events.add("message " + message);
                done.countDown();
            })
            .onClose((_, _, _) -> {
                closeStarted.countDown();
                events.add("close");
                done.countDown();
            });

        EmbeddedChannel channel = Util.connect(server);
        send(channel, "first", "second");
        channel.close();

        assertThat(done.await(5, TimeUnit.SECONDS)).as("Callbacks should be called").isTrue();
        assertThat(events).as("Close callback should run after the queued messages").isEqualTo(List.of("message first", "message second", "close"));
    }

    @Test
    void shouldPassRejectedTaskToItsHandlerWithoutStallingSerialExecutor() {
        AtomicBoolean rejecting = new AtomicBoolean(true);
        List<String> executed = new ArrayList<>();
        List<String> rejected = new ArrayList<>();

        // Rejects tasks like a shutdown executor until told otherwise, then runs them right away
        SerialExecutor executor = new SerialExecutor(task -> {
            if (rejecting.get()) {
                throw new RejectedExecutionException("Executor is shut down");
            }

            task.run();
        });

        executor.execute(() -> executed.add("rejected"), _ -> rejected.add("rejected"));
        rejecting.set(false);
        executor.execute(() -> executed.add("accepted"), _ -> rejected.add("accepted"));

        assertThat(rejected).as("Rejected task should reach its handler").containsExactly("rejected");
        assertThat(executed).as("Tasks submitted after a rejection should still run").containsExactly("accepted");
    }

    @Test
    void shouldPassQueuedTasksToTheirHandlersWhenExecutorRejectsThemLater() {
        Queue<Runnable> accepted = new ArrayDeque<>();
        AtomicBoolean rejecting = new AtomicBoolean();
        List<String> executed = new ArrayList<>();
        List<String> rejected = new ArrayList<>();
        SerialExecutor executor = new SerialExecutor(task -> {
            if (rejecting.get()) {
                throw new RejectedExecutionException("Executor is shut down");
            }

            accepted.add(task);
        });

        executor.execute(() -> executed.add("first"), _ -> rejected.add("first"));
        executor.execute(() -> executed.add("second"), _ -> rejected.add("second"));
        executor.execute(() -> executed.add("third"), _ -> rejected.add("third"));

        // The executor shuts down while the first task runs
        rejecting.set(true);
        accepted.remove().run();

        assertThat(executed).as("Only the first task should run").containsExactly("first");
        assertThat(rejected).as("Every later task should reach its handler in order").containsExactly("second", "third");
    }

    @Test
    @Timeout(5)
    void shouldAcceptTasksWhileExecutorRunsTaskOnCallingThread() throws Exception {
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SerialExecutor executor = new SerialExecutor(Runnable::run);
        Thread worker = Thread.ofPlatform().start(() -> executor.execute(() -> {
            running.countDown();
            awaitQuietly(release, 5000);
        }, _ -> {}));

        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();

        // Submitting must not wait for the task the executor runs on the worker thread
        executor.execute(() -> {}, _ -> {});
        release.countDown();
        worker.join();
    }
}
