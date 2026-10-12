package pl.mbaracz.pongwire;

import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicBrokerTest {

    private static final class RecordingTopicBroker implements TopicBroker<String> {

        private final List<String> calls = new ArrayList<>();
        private final List<TopicMessageHandler<String>> handlers = new ArrayList<>();
        private CompletionStage<Void> subscribeResult = CompletableFuture.completedFuture(null);
        private CompletionStage<Void> unsubscribeResult = CompletableFuture.completedFuture(null);
        private CompletionStage<Void> publishResult = CompletableFuture.completedFuture(null);
        private Throwable publishFailure;

        @Override
        public CompletionStage<Void> subscribe(String topic, TopicMessageHandler<String> handler) {
            calls.add("subscribe " + topic);
            handlers.add(handler);
            return subscribeResult;
        }

        @Override
        public CompletionStage<Void> unsubscribe(String topic, TopicMessageHandler<String> handler) {
            calls.add("unsubscribe " + topic);
            handlers.add(handler);
            return unsubscribeResult;
        }

        @Override
        public CompletionStage<Void> publish(String topic, String message) {
            calls.add("publish " + topic + " " + message);
            return publishFailure == null
                ? publishResult
                : CompletableFuture.failedFuture(publishFailure);
        }
    }

    private final RecordingTopicBroker broker = new RecordingTopicBroker();
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            )
            .topicBroker(broker);
    }

    @Test
    void shouldSubscribeBrokerForFirstLocalSessionAndUnsubscribeAfterLast() {
        EmbeddedChannel firstChannel = connect(DefaultChannelId.newInstance());
        EmbeddedChannel secondChannel = connect(DefaultChannelId.newInstance());
        WebSocketSession<String, Object> firstSession = server.getSessionByChannelId(firstChannel.id());
        WebSocketSession<String, Object> secondSession = server.getSessionByChannelId(secondChannel.id());

        server.subscribe(firstSession, "topic").toCompletableFuture().join();
        server.subscribe(secondSession, "topic").toCompletableFuture().join();
        server.unsubscribe(firstSession, "topic").toCompletableFuture().join();

        assertThat(broker.calls).containsExactly("subscribe topic");
        assertThat(server.isSubscribed(firstSession, "topic")).isFalse();
        assertThat(server.isSubscribed(secondSession, "topic")).isTrue();
        assertThat(server.getTopics()).containsExactly("topic");

        server.unsubscribe(secondSession, "topic").toCompletableFuture().join();

        assertThat(broker.calls).containsExactly("subscribe topic", "unsubscribe topic");
        assertThat(broker.handlers).hasSize(2).allMatch(handler -> handler == broker.handlers.getFirst());
        assertThat(server.getTopics()).isEmpty();
    }

    @Test
    void shouldUseBrokerOnlyForTransport() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());

        server.subscribe(session, "topic").toCompletableFuture().join();
        server.publish("topic", "Hello").toCompletableFuture().join();
        server.unsubscribeAllTopics().toCompletableFuture().join();

        assertThat(broker.calls)
            .containsExactly("subscribe topic", "publish topic Hello", "unsubscribe topic");
        assertThat(server.isSubscribed(session, "topic")).isFalse();
        assertThat(server.getTopics()).isEmpty();
    }

    @Test
    void shouldUnsubscribeBrokerWhenLastSessionDisconnects() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        server.subscribe(session, "topic").toCompletableFuture().join();

        channel.close();

        assertThat(broker.calls).containsExactly("subscribe topic", "unsubscribe topic");
        assertThat(server.getTopics()).isEmpty();
    }

    @Test
    void shouldExposeBrokerFailureThroughReturnedStage() {
        IllegalStateException failure = new IllegalStateException("broker unavailable");
        broker.publishFailure = failure;

        CompletableFuture<Void> publish = server.publish("topic", "message").toCompletableFuture();

        assertThatThrownBy(publish::join).hasCause(failure);
    }

    @Test
    void shouldShareFailedSubscribeAndAllowRetry() {
        EmbeddedChannel firstChannel = connect(DefaultChannelId.newInstance());
        EmbeddedChannel secondChannel = connect(DefaultChannelId.newInstance());
        WebSocketSession<String, Object> firstSession = server.getSessionByChannelId(firstChannel.id());
        WebSocketSession<String, Object> secondSession = server.getSessionByChannelId(secondChannel.id());
        CompletableFuture<Void> brokerAttempt = new CompletableFuture<>();
        broker.subscribeResult = brokerAttempt;

        CompletionStage<Void> first = server.subscribe(firstSession, "topic");
        CompletionStage<Void> second = server.subscribe(secondSession, "topic");

        // Both stages follow the one broker subscription
        assertThat(broker.calls).containsExactly("subscribe topic");

        IllegalStateException failure = new IllegalStateException("subscribe failed");
        brokerAttempt.completeExceptionally(failure);
        assertThatThrownBy(() -> first.toCompletableFuture().join()).hasCause(failure);
        assertThatThrownBy(() -> second.toCompletableFuture().join()).hasCause(failure);

        RecordingTopicBroker replacement = new RecordingTopicBroker();
        server.topicBroker(replacement);
        server.subscribe(secondSession, "topic").toCompletableFuture().join();

        assertThat(broker.calls).containsExactly("subscribe topic");
        assertThat(replacement.calls).containsExactly("subscribe topic");
        assertThat(server.isSubscribed(firstSession, "topic")).as("Failed subscription should be rolled back").isFalse();
        assertThat(server.isSubscribed(secondSession, "topic")).as("Retried subscription should succeed").isTrue();
    }

    @Test
    void shouldPreventBrokerReplacementWhilePublishIsPending() {
        CompletableFuture<Void> pendingPublish = new CompletableFuture<>();
        broker.publishResult = pendingPublish;
        RecordingTopicBroker replacement = new RecordingTopicBroker();

        CompletionStage<Void> publish = server.publish("topic", "message");

        assertThatThrownBy(() -> server.topicBroker(replacement))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Topic broker cannot be replaced while subscriptions or operations are still active");

        pendingPublish.complete(null);
        publish.toCompletableFuture().join();
        server.topicBroker(replacement);

        assertThat(broker.calls).containsExactly("publish topic message");
        assertThat(replacement.calls).isEmpty();
    }

    @Test
    void shouldRetryOrphanedBrokerSubscriptionWhenUnsubscribingAllTopics() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        server.subscribe(session, "topic").toCompletableFuture().join();
        IllegalStateException failure = new IllegalStateException("unsubscribe failed");
        broker.unsubscribeResult = CompletableFuture.failedFuture(failure);

        CompletionStage<Void> firstUnsubscribe = server.unsubscribe(session, "topic");

        assertThatThrownBy(() -> firstUnsubscribe.toCompletableFuture().join()).hasCause(failure);
        assertThat(server.getTopics()).isEmpty();

        RecordingTopicBroker replacement = new RecordingTopicBroker();
        assertThatThrownBy(() -> server.topicBroker(replacement))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Topic broker cannot be replaced while subscriptions or operations are still active");

        broker.unsubscribeResult = CompletableFuture.completedFuture(null);
        server.unsubscribeAllTopics().toCompletableFuture().join();
        server.topicBroker(replacement);

        assertThat(broker.calls)
            .containsExactly("subscribe topic", "unsubscribe topic", "unsubscribe topic");
    }

    @Test
    void shouldReuseSubscriptionWhenPendingUnsubscribeFails() {
        EmbeddedChannel firstChannel = connect(DefaultChannelId.newInstance());
        EmbeddedChannel secondChannel = connect(DefaultChannelId.newInstance());
        WebSocketSession<String, Object> firstSession = server.getSessionByChannelId(firstChannel.id());
        WebSocketSession<String, Object> secondSession = server.getSessionByChannelId(secondChannel.id());
        server.subscribe(firstSession, "topic").toCompletableFuture().join();
        CompletableFuture<Void> brokerUnsubscribe = new CompletableFuture<>();
        broker.unsubscribeResult = brokerUnsubscribe;

        CompletionStage<Void> unsubscribe = server.unsubscribe(firstSession, "topic");
        CompletionStage<Void> subscribe = server.subscribe(secondSession, "topic");

        IllegalStateException failure = new IllegalStateException("unsubscribe failed");
        brokerUnsubscribe.completeExceptionally(failure);

        assertThatThrownBy(() -> unsubscribe.toCompletableFuture().join()).hasCause(failure);
        subscribe.toCompletableFuture().join();
        assertThat(broker.calls).containsExactly("subscribe topic", "unsubscribe topic");
        assertThat(server.isSubscribed(secondSession, "topic")).isTrue();
    }

    @Test
    void shouldResubscribeWhenPendingUnsubscribeSucceeds() {
        EmbeddedChannel firstChannel = connect(DefaultChannelId.newInstance());
        EmbeddedChannel secondChannel = connect(DefaultChannelId.newInstance());
        WebSocketSession<String, Object> firstSession = server.getSessionByChannelId(firstChannel.id());
        WebSocketSession<String, Object> secondSession = server.getSessionByChannelId(secondChannel.id());
        server.subscribe(firstSession, "topic").toCompletableFuture().join();
        CompletableFuture<Void> brokerUnsubscribe = new CompletableFuture<>();
        broker.unsubscribeResult = brokerUnsubscribe;

        CompletionStage<Void> unsubscribe = server.unsubscribe(firstSession, "topic");
        CompletionStage<Void> subscribe = server.subscribe(secondSession, "topic");
        brokerUnsubscribe.complete(null);

        unsubscribe.toCompletableFuture().join();
        subscribe.toCompletableFuture().join();
        assertThat(broker.calls)
            .containsExactly("subscribe topic", "unsubscribe topic", "subscribe topic");
        assertThat(server.isSubscribed(secondSession, "topic")).isTrue();
    }

    private EmbeddedChannel connect(DefaultChannelId id) {
        EmbeddedChannel channel = Util.newEmbeddedChannel(id, new WebSocketServerChannelInitializer<>(server));
        Util.performHandshake(channel, "/");
        channel.releaseOutbound();
        return channel;
    }

    @Test
    void shouldSubscribeBrokerAgainAfterRestartWhenUnsubscribeFailedDuringStop() {
        server.listen(0);
        EmbeddedChannel channel = Util.connect(server);
        server.subscribe(server.getSessionByChannelId(channel.id()), "topic").toCompletableFuture().join();

        // The broker fails to unsubscribe the closed session, e.g. because its client is shut down
        broker.unsubscribeResult = CompletableFuture.failedFuture(new IllegalStateException("broker closed"));
        server.stop();

        assertThatCode(() -> server.topicBroker(new RecordingTopicBroker())).as("Broker should be replaceable").doesNotThrowAnyException();

        server.topicBroker(broker).listen(0);
        EmbeddedChannel restarted = Util.connect(server);
        server.subscribe(server.getSessionByChannelId(restarted.id()), "topic").toCompletableFuture().join();
        server.stop();

        assertThat(broker.calls).as("Topic should be subscribed again").containsSubsequence("subscribe topic", "unsubscribe topic", "subscribe topic");
    }

    @Test
    void shouldKeepTrackingBrokerOperationWhenCallerCompletesReturnedStage() {
        broker.publishResult = new CompletableFuture<>();

        // A caller giving up on the publish, e.g. with orTimeout() or complete()
        server.publish("topic", "message").toCompletableFuture().complete(null);

        assertThatThrownBy(() -> server.topicBroker(new RecordingTopicBroker()))
            .as("The broker operation should still be pending")
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldRollBackLocalSubscriptionWhenBrokerSubscribeFails() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        broker.subscribeResult = CompletableFuture.failedFuture(new IllegalStateException("broker unavailable"));

        CompletableFuture<Void> subscribe = server.subscribe(session, "topic").toCompletableFuture();

        assertThat(subscribe).as("Subscribe should fail").isCompletedExceptionally();
        assertThat(server.isSubscribed(session, "topic")).as("Session should not stay subscribed").isFalse();
        assertThat(server.getTopics()).as("Topic should not stay registered").isEmpty();
    }

    @Test
    void shouldFailBrokerOperationThatDoesNotCompleteInTime() {
        server.configure(configurer -> configurer.setBrokerOperationTimeout(Duration.ofMillis(100)));
        broker.publishResult = new CompletableFuture<>();

        CompletableFuture<Void> stuck = server.publish("topic", "first").toCompletableFuture();

        assertThat(stuck).as("Stuck publish should fail").failsWithin(5, TimeUnit.SECONDS);

        // The next operation of the topic is no longer blocked
        broker.publishResult = CompletableFuture.completedFuture(null);
        assertThat(server.publish("topic", "second").toCompletableFuture()).succeedsWithin(5, TimeUnit.SECONDS);
    }
}
