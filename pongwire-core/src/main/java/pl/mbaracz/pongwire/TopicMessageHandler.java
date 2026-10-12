package pl.mbaracz.pongwire;

/**
 * Handles a message received from a topic broker.
 *
 * @param <T> the type of topic messages.
 */
@FunctionalInterface
public interface TopicMessageHandler<T> {

    /**
     * Handles a message received from a topic.
     *
     * @param topic   the topic.
     * @param message the received message.
     */
    void onMessage(String topic, T message);
}
