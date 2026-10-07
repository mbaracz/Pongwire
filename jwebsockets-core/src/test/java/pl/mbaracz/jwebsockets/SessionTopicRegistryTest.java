package pl.mbaracz.jwebsockets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

class SessionTopicRegistryTest {

    private final WebSocketSession<String, Object> alice = new WebSocketSession<>(null, null, null, null);
    private final WebSocketSession<String, Object> bob = new WebSocketSession<>(null, null, null, null);
    private SessionTopicRegistry<String, Object> registry;

    @BeforeEach
    void setUp() {
        registry = new SessionTopicRegistry<>();
    }

    @Test
    void shouldReturnOnlyTopicsLeftWithoutSubscribersWhenSessionUnsubscribesFromAll() {
        registry.subscribe(alice, "own");
        registry.subscribe(alice, "shared");
        registry.subscribe(bob, "shared");
        registry.subscribe(bob, "other");

        assertThat(registry.unsubscribeAll(alice)).as("Topics left without subscribers").containsExactly("own");
        assertThat(registry.getTopics()).as("Remaining topics").containsExactlyInAnyOrder("shared", "other");
        assertThat(registry.getSubscribers("shared")).as("Remaining subscribers").containsExactly(bob);
    }

    @Test
    void shouldReturnNoTopicsWhenSessionHasNoSubscriptions() {
        registry.subscribe(bob, "topic");

        assertThat(registry.unsubscribeAll(alice)).as("Topics left without subscribers").isEmpty();
        assertThat(registry.getSubscribers("topic")).as("Subscribers").containsExactly(bob);
    }

    @Test
    void shouldForgetTopicOfSessionWhenItUnsubscribes() {
        registry.subscribe(alice, "first");
        registry.subscribe(alice, "second");

        registry.unsubscribe(alice, "first");

        assertThat(registry.isSubscribed(alice, "first")).as("Should not be subscribed").isFalse();
        assertThat(registry.hasSubscribers("first")).as("Topic should have no subscribers").isFalse();
        assertThat(registry.unsubscribeAll(alice)).as("Topics left without subscribers").containsExactly("second");
    }

    @Test
    void shouldReuseSubscribersUntilSubscriptionsOfTopicChange() {
        registry.subscribe(alice, "topic");

        Collection<WebSocketSession<String, Object>> first = registry.getSubscribers("topic");
        registry.subscribe(bob, "other");

        assertThat(registry.getSubscribers("topic")).as("Should not copy again").isSameAs(first);

        registry.subscribe(bob, "topic");

        assertThat(registry.getSubscribers("topic")).as("Should contain the new subscriber").containsExactlyInAnyOrder(alice, bob);
        assertThat(first).as("Earlier snapshot should not change").containsExactly(alice);
    }

    @Test
    void shouldReportSubscribersOfTopic() {
        assertThat(registry.hasSubscribers("topic")).as("Unknown topic").isFalse();

        registry.subscribe(alice, "topic");

        assertThat(registry.hasSubscribers("topic")).as("Subscribed topic").isTrue();
        assertThat(registry.getSubscribers("unknown")).as("Unknown topic").isEmpty();
    }
}
