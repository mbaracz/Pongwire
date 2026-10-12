package pl.mbaracz.pongwire;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * Topic broker transporting messages within this JVM.
 *
 * @param <T> the type of topic messages.
 */
final class InMemoryTopicBroker<T> implements TopicBroker<T> {

    private static final int DELIVERY_STRIPES = 64;

    private final Map<String, Set<TopicMessageHandler<T>>> topics = new ConcurrentHashMap<>();
    private final SerialDelivery[] deliveries = IntStream.range(0, DELIVERY_STRIPES)
        .mapToObj(_ -> new SerialDelivery())
        .toArray(SerialDelivery[]::new);

    @Override
    public CompletionStage<Void> subscribe(String topic, TopicMessageHandler<T> handler) {
        Objects.requireNonNull(topic, "Topic must not be null!");
        Objects.requireNonNull(handler, "Topic message handler must not be null!");
        topics.compute(topic, (_, handlers) -> {
            if (handlers == null) {
                handlers = ConcurrentHashMap.newKeySet();
            }
            handlers.add(handler);
            return handlers;
        });
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> unsubscribe(String topic, TopicMessageHandler<T> handler) {
        Objects.requireNonNull(topic, "Topic must not be null!");
        Objects.requireNonNull(handler, "Topic message handler must not be null!");
        topics.computeIfPresent(topic, (_, handlers) -> {
            handlers.remove(handler);
            return handlers.isEmpty() ? null : handlers;
        });
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> publish(String topic, T message) {
        Objects.requireNonNull(topic, "Topic must not be null!");

        CompletableFuture<Void> result = new CompletableFuture<>();

        deliveryFor(topic).execute(() -> {
            try {
                topics.getOrDefault(topic, Collections.emptySet())
                    .forEach(handler -> handler.onMessage(topic, message));
                result.complete(null);
            } catch (Throwable throwable) {
                result.completeExceptionally(throwable);
            }
        });
        return result;
    }

    private SerialDelivery deliveryFor(String topic) {
        return deliveries[Math.floorMod(topic.hashCode(), deliveries.length)];
    }

    private static final class SerialDelivery {

        private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean draining = new AtomicBoolean();

        private void execute(Runnable task) {
            tasks.add(task);
            if (draining.compareAndSet(false, true)) {
                drain();
            }
        }

        private void drain() {
            do {
                Runnable task;
                while ((task = tasks.poll()) != null) {
                    task.run();
                }
                draining.set(false);
            } while (!tasks.isEmpty() && draining.compareAndSet(false, true));
        }
    }
}
