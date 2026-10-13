package pl.mbaracz.pongwire;

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

    // The topics of each session with the attempt that subscribed them, so a closing session is removed
    // only from its own topics and a failed attempt rolls back only its own membership
    private final Map<WebSocketSession<T, D>, Map<String, Long>> sessionTopics = new HashMap<>();
    private long attempts;

    // Subscribers of each topic as returned to publishers, rebuilt only after the subscriptions of the topic change
    private final Map<String, List<WebSocketSession<T, D>>> subscriberSnapshots = new HashMap<>();

    /**
     * Subscribes the session to the topic.
     *
     * @return the attempt that owns the membership, for {@link #unsubscribe(WebSocketSession, String, long)}.
     */
    synchronized long subscribe(WebSocketSession<T, D> session, String topic) {
        Objects.requireNonNull(session, "WebSocket session must not be null!");
        Objects.requireNonNull(topic, "Topic must not be null!");
        long attempt = ++attempts;

        if (topics.computeIfAbsent(topic, _ -> new HashSet<>()).add(session)) {
            subscriberSnapshots.remove(topic);
        }
        sessionTopics.computeIfAbsent(session, _ -> new HashMap<>()).put(topic, attempt);
        return attempt;
    }

    synchronized void unsubscribe(WebSocketSession<T, D> session, String topic) {
        unsubscribe(session, topic, 0);
    }

    /**
     * Unsubscribes the session from the topic, unless a later attempt subscribed it again meanwhile.
     *
     * @param attempt the attempt that subscribed the session, or 0 to unsubscribe whatever the attempt.
     */
    synchronized void unsubscribe(WebSocketSession<T, D> session, String topic, long attempt) {
        Objects.requireNonNull(topic, "Topic must not be null!");
        Map<String, Long> subscribedTopics = sessionTopics.get(session);

        if (subscribedTopics == null || attempt != 0 && !Long.valueOf(attempt).equals(subscribedTopics.get(topic))) {
            return;
        }

        if (subscribedTopics.remove(topic) != null) {
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
        Map<String, Long> subscribedTopics = sessionTopics.remove(session);
        Set<String> emptyTopics = new HashSet<>();

        if (subscribedTopics != null) {
            for (String topic : subscribedTopics.keySet()) {
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
