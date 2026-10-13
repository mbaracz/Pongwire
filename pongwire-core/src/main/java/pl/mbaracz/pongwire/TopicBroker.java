package pl.mbaracz.pongwire;

import java.util.concurrent.CompletionStage;

/**
 * Broker for topic messages. Local session subscriptions are managed by {@link WebSocketServer}.
 * Implementations must be thread-safe and must not perform blocking I/O on the calling thread.
 * Calls to a handler for the same topic must be ordered and must not overlap.
 * Different topics may be delivered concurrently.
 *
 * The server may call the next operation of a topic from the thread that completes the stage of the previous one,
 * so do not complete a stage while holding a lock that the broker methods also take.
 * {@link TopicMessageHandler#onMessage} encodes the message and queues it for the sessions without blocking,
 * so it may be called from any thread, including an I/O thread of the broker.
 *
 * The server gives up on an operation after the broker operation timeout and then forgets the subscription.
 * It may subscribe the topic again with a new handler while the broker still completes the old operation,
 * and it unsubscribes a handler the broker may have dropped already, so unsubscribing an unknown handler must
 * succeed and a handler must not be shared between subscriptions.
 *
 * Broker lifecycle is owned by the application.
 *
 * @param <T> the topic message type
 */
public interface TopicBroker<T> {

    /**
     * Subscribes a message handler to the topic.
     * If the returned stage fails, the handler must be considered unsubscribed.
     *
     * @param topic   the topic.
     * @param handler the handler to subscribe.
     * @return a stage completed when the subscription has been established.
     */
    CompletionStage<Void> subscribe(String topic, TopicMessageHandler<T> handler);

    /**
     * Unsubscribes a message handler from the topic.
     * If the returned stage fails, the handler must remain logically subscribed.
     * After successful completion, the handler must not receive new messages for the topic.
     *
     * @param topic   the topic.
     * @param handler the handler to unsubscribe.
     * @return a stage completed when the subscription has been removed.
     */
    CompletionStage<Void> unsubscribe(String topic, TopicMessageHandler<T> handler);

    /**
     * Publishes a message to the topic.
     *
     * @param topic   the topic.
     * @param message the message to publish.
     * @return a stage completed when the broker has accepted the message.
     */
    CompletionStage<Void> publish(String topic, T message);
}
