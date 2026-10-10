package pl.mbaracz.jwebsockets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class PubSubConcurrencyTest {

    private static final String TOPIC = "topic";
    private static final int THREADS = 8;
    private static final int ITERATIONS = 10_000;

    private SessionTopicRegistry<String, Object> registry;

    @BeforeEach
    void setUp() {
        registry = new SessionTopicRegistry<>();
    }

    private static List<WebSocketSession<String, Object>> createSessions(int count) {
        return IntStream.range(0, count)
            .mapToObj(_ -> new WebSocketSession<String, Object>(null, null, null, null, null))
            .toList();
    }

    private static void runConcurrently(IntConsumer task) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);

        try {
            List<Future<?>> futures = new ArrayList<>();

            for (int thread = 0; thread < THREADS; thread++) {
                int index = thread;
                futures.add(executor.submit(() -> {
                    start.await();
                    task.accept(index);
                    return null;
                }));
            }

            start.countDown();

            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldNotLoseSubscriptionWhenSessionsSubscribeConcurrently() throws Exception {
        List<WebSocketSession<String, Object>> sessions = createSessions(THREADS * 1_000);

        runConcurrently(thread -> {
            for (int i = thread; i < sessions.size(); i += THREADS) {
                registry.subscribe(sessions.get(i), TOPIC);
            }
        });

        assertThat(sessions).allMatch(session -> registry.isSubscribed(session, TOPIC));
        assertThat(registry.getSubscribers(TOPIC)).containsExactlyInAnyOrderElementsOf(sessions);
    }

    @Test
    void shouldProvideSafeSnapshotsDuringSubscriptionChanges() throws Exception {
        List<WebSocketSession<String, Object>> sessions = createSessions(THREADS);

        runConcurrently(thread -> {
            WebSocketSession<String, Object> session = sessions.get(thread);

            for (int i = 0; i < ITERATIONS; i++) {
                registry.subscribe(session, TOPIC);
                registry.getSubscribers(TOPIC).forEach(subscriber -> registry.isSubscribed(subscriber, TOPIC));
                registry.unsubscribe(session, TOPIC);
            }
        });

        assertThat(registry.getTopics()).doesNotContain(TOPIC);
    }

    @Test
    void shouldKeepOtherSubscriptionsWhenSessionsUnsubscribeConcurrently() throws Exception {
        List<WebSocketSession<String, Object>> sessions = createSessions(THREADS);

        runConcurrently(thread -> {
            WebSocketSession<String, Object> session = sessions.get(thread);

            for (int i = 0; i < ITERATIONS; i++) {
                registry.subscribe(session, TOPIC);
                assertThat(registry.isSubscribed(session, TOPIC)).isTrue();
                registry.unsubscribe(session, TOPIC);
            }
        });
    }
}
