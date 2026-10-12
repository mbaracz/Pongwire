package pl.mbaracz.pongwire;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryTopicBrokerTest {

    @Test
    void shouldDeliverMessagesInPublishOrder() {
        InMemoryTopicBroker<String> broker = new InMemoryTopicBroker<>();
        List<String> messages = new ArrayList<>();
        broker.subscribe("chat", (_, message) -> messages.add(message));

        broker.publish("chat", "a").toCompletableFuture().join();
        broker.publish("chat", "b").toCompletableFuture().join();
        broker.publish("chat", "c").toCompletableFuture().join();

        assertThat(messages).containsExactly("a", "b", "c");
    }

    @Test
    void shouldNotOverlapDeliveriesForTheSameTopic() throws InterruptedException {
        InMemoryTopicBroker<String> broker = new InMemoryTopicBroker<>();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondPublishReturned = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        broker.subscribe("chat", (_, message) -> {
            if (message.equals("a")) {
                firstStarted.countDown();
                await(releaseFirst);
            } else {
                secondStarted.countDown();
            }
        });

        CompletableFuture<Void> firstPublish = CompletableFuture.runAsync(() ->
            broker.publish("chat", "a").toCompletableFuture().join()
        );
        assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<Void> secondPublish = CompletableFuture.runAsync(() -> {
            var result = broker.publish("chat", "b");
            secondPublishReturned.countDown();
            result.toCompletableFuture().join();
        });
        try {
            assertThat(secondPublishReturned.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(secondStarted.await(200, TimeUnit.MILLISECONDS)).isFalse();
        } finally {
            releaseFirst.countDown();
        }
        CompletableFuture.allOf(firstPublish, secondPublish).join();

        assertThat(secondStarted.getCount()).isZero();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }
}
