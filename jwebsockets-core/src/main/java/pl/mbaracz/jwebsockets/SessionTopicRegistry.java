package pl.mbaracz.jwebsockets;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Keeps the local topic subscriptions of WebSocket sessions.
 */
final class SessionTopicRegistry<T, D> {

    private final Map<String, Set<WebSocketSession<T, D>>> topics = new HashMap<>();

    // The topics of each session, so a closing session is removed only from its own topics
    private final Map<WebSocketSession<T, D>, Set<String>> sessionTopics = new HashMap<>();

    // Subscribers of each topic as returned to publishers, rebuilt only after the subscriptions of the topic change
    private final Map<String, List<WebSocketSession<T, D>>> subscriberSnapshots = new HashMap<>();

    synchronized void subscribe(WebSocketSession<T, D> session, String topic) {
        Objects.requireNonNull(session, "WebSocket session must not be null!");
        Objects.requireNonNull(topic, "Topic must not be null!");

        if (topics.computeIfAbsent(topic, _ -> new HashSet<>()).add(session)) {
            sessionTopics.computeIfAbsent(session, _ -> new HashSet<>()).add(topic);
            subscriberSnapshots.remove(topic);
        }
    }

    synchronized void unsubscribe(WebSocketSession<T, D> session, String topic) {
        Objects.requireNonNull(topic, "Topic must not be null!");
        Set<String> subscribedTopics = sessionTopics.get(session);

        if (subscribedTopics != null && subscribedTopics.remove(topic)) {
            if (subscribedTopics.isEmpty()) {
                sessionTopics.remove(session);
            }

            removeSubscriber(session, topic);
        }
    }

    /**
     * Unsubscribes the session from all its topics.
     *
     * @param session the session to unsubscribe.
     * @return the topics left without subscribers.
     */
    synchronized Set<String> unsubscribeAll(WebSocketSession<T, D> session) {
        Set<String> subscribedTopics = sessionTopics.remove(session);
        Set<String> emptyTopics = new HashSet<>();

        if (subscribedTopics != null) {
            for (String topic : subscribedTopics) {
                if (removeSubscriber(session, topic)) {
                    emptyTopics.add(topic);
                }
            }
        }

        return emptyTopics;
    }

    synchronized void clear() {
        topics.clear();
        sessionTopics.clear();
        subscriberSnapshots.clear();
    }

    synchronized boolean isSubscribed(WebSocketSession<T, D> session, String topic) {
        Set<WebSocketSession<T, D>> subscribers = topics.get(topic);
        return subscribers != null && subscribers.contains(session);
    }

    synchronized boolean hasSubscribers(String topic) {
        return topics.containsKey(topic);
    }

    synchronized Set<String> getTopics() {
        return Set.copyOf(topics.keySet());
    }

    synchronized Collection<WebSocketSession<T, D>> getSubscribers(String topic) {
        Set<WebSocketSession<T, D>> subscribers = topics.get(topic);

        if (subscribers == null) {
            return List.of();
        }

        return subscriberSnapshots.computeIfAbsent(topic, _ -> List.copyOf(subscribers));
    }

    /**
     * Removes the session from the subscribers of the topic, and the topic once it has none.
     *
     * @return true if the topic has no subscribers left.
     */
    private boolean removeSubscriber(WebSocketSession<T, D> session, String topic) {
        Set<WebSocketSession<T, D>> subscribers = topics.get(topic);
        subscriberSnapshots.remove(topic);
        subscribers.remove(session);

        if (subscribers.isEmpty()) {
            topics.remove(topic);
            return true;
        }

        return false;
    }
}
