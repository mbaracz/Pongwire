package pl.mbaracz.pongwire;

import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class TopicBrokerTest {

    private static final class RecordingTopicBroker implements TopicBroker<String> {

        // Recorded from the test thread and from the timeout thread
        private final List<String> calls = new CopyOnWriteArrayList<>();
        private final List<TopicMessageHandler<String>> handlers = new CopyOnWriteArrayList<>();
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
    void shouldNotDeliverMessagesFromReplacedBrokerThatKeptItsHandler() {
        server.listen(0);
        EmbeddedChannel channel = Util.connect(server);
        server.subscribe(server.getSessionByChannelId(channel.id()), "topic").toCompletableFuture().join();
        TopicMessageHandler<String> retained = broker.handlers.getFirst();

        // The broker fails to unsubscribe the closed session and keeps the handler
        broker.unsubscribeResult = CompletableFuture.failedFuture(new IllegalStateException("broker closed"));
        server.stop();
        server.topicBroker(new RecordingTopicBroker()).listen(0);
        EmbeddedChannel restarted = Util.connect(server);
        server.subscribe(server.getSessionByChannelId(restarted.id()), "topic").toCompletableFuture().join();

        retained.onMessage("topic", "stale");
        restarted.runPendingTasks();

        assertThat(Util.readFromServer(restarted)).as("The retired broker should deliver nothing").isNull();
        server.stop();
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
    void shouldRollBackOnlyFailedAttemptWhenSessionSubscribedAgainMeanwhile() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        CompletableFuture<Void> first = new CompletableFuture<>();
        broker.subscribeResult = first;

        // The session unsubscribes and subscribes again while the first broker subscription is pending
        CompletableFuture<Void> firstSubscribe = server.subscribe(session, "topic").toCompletableFuture();
        server.unsubscribe(session, "topic");
        broker.subscribeResult = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> secondSubscribe = server.subscribe(session, "topic").toCompletableFuture();
        first.completeExceptionally(new IllegalStateException("broker unavailable"));

        assertThat(firstSubscribe).as("First subscribe").isCompletedExceptionally();
        assertThat(secondSubscribe).as("Second subscribe").succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(server.isSubscribed(session, "topic")).as("The second attempt should keep its membership").isTrue();
        assertThat(broker.calls).as("Broker calls").containsExactly("subscribe topic", "subscribe topic");
    }

    @Test
    void shouldKeepBrokerSubscribedWhenOlderAttemptRollsBackAfterFailedUnsubscribe() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        CompletableFuture<Void> first = new CompletableFuture<>();
        broker.subscribeResult = first;

        // A pending, unsubscribe, B, unsubscribe again, the last broker unsubscribe fails and keeps the handler
        server.subscribe(session, "topic");
        server.unsubscribe(session, "topic");
        broker.subscribeResult = CompletableFuture.completedFuture(null);
        server.subscribe(session, "topic");
        broker.unsubscribeResult = CompletableFuture.failedFuture(new IllegalStateException("broker unavailable"));
        server.unsubscribe(session, "topic");
        first.completeExceptionally(new IllegalStateException("broker unavailable"));

        // The topic is still subscribed with the broker, so the next subscriber reuses it
        broker.subscribeResult = CompletableFuture.completedFuture(null);
        server.subscribe(session, "topic").toCompletableFuture().join();

        assertThat(broker.calls).as("Broker calls").containsExactly("subscribe topic", "subscribe topic", "unsubscribe topic");
    }

    @Test
    void shouldCompleteSubscribeStageAfterRollingBackFailedSubscription() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        CompletableFuture<Void> pending = new CompletableFuture<>();
        broker.subscribeResult = pending;
        AtomicBoolean subscribedWhenFailed = new AtomicBoolean(true);

        server.subscribe(session, "topic").whenComplete((_, _) -> subscribedWhenFailed.set(server.isSubscribed(session, "topic")));
        pending.completeExceptionally(new IllegalStateException("broker unavailable"));

        assertThat(subscribedWhenFailed).as("The failure continuation should see the session unsubscribed").isFalse();
    }

    @Test
    void shouldSubscribeAgainWithNewHandlerWhenUnsubscribeTimedOut() {
        server.configure(configurer -> configurer.setBrokerOperationTimeout(Duration.ofMillis(100)));
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        server.subscribe(session, "topic").toCompletableFuture().join();
        CompletableFuture<Void> unsubscribing = new CompletableFuture<>();
        broker.unsubscribeResult = unsubscribing;

        CompletableFuture<Void> unsubscribe = server.unsubscribe(session, "topic").toCompletableFuture();

        assertThat(unsubscribe).as("Unsubscribe should time out").failsWithin(5, TimeUnit.SECONDS);

        // The broker removes the handler after all, so the old subscription must not be reused
        unsubscribing.complete(null);
        server.subscribe(session, "topic").toCompletableFuture().join();

        assertThat(broker.calls).as("Broker calls").containsExactly("subscribe topic", "unsubscribe topic", "subscribe topic");
        assertThat(broker.handlers.get(2)).as("A new handler should be subscribed").isNotSameAs(broker.handlers.get(0));
    }

    @Test
    void shouldDropHandlerWhenSubscribeSucceedsAfterTimeout() {
        server.configure(configurer -> configurer.setBrokerOperationTimeout(Duration.ofMillis(100)));
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        CompletableFuture<Void> subscribing = new CompletableFuture<>();
        broker.subscribeResult = subscribing;

        CompletableFuture<Void> subscribe = server.subscribe(session, "topic").toCompletableFuture();

        assertThat(subscribe).as("Subscribe should time out").failsWithin(5, TimeUnit.SECONDS);

        subscribing.complete(null);
        broker.handlers.getFirst().onMessage("topic", "late");
        channel.runPendingTasks();

        assertThat(broker.calls).as("Broker calls").containsExactly("subscribe topic", "unsubscribe topic");
        assertThat(broker.handlers.get(1)).as("The late handler should be unsubscribed").isSameAs(broker.handlers.getFirst());
        assertThat(Util.readFromServer(channel)).as("The late handler should deliver nothing").isNull();
    }

    @Test
    void shouldKeepHandlerFailedUnsubscribeLeftWhenEarlierResubscribeInstalledNewOne() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        server.subscribe(session, "topic").toCompletableFuture().join();
        CompletableFuture<Void> firstUnsubscribe = new CompletableFuture<>();
        broker.unsubscribeResult = firstUnsubscribe;

        // Unsubscribe, subscribe, unsubscribe, subscribe queue up while the first unsubscribe is pending,
        // the second unsubscribe fails and leaves the handler of the resubscribe with the broker
        server.unsubscribe(session, "topic");
        server.subscribe(session, "topic");
        broker.unsubscribeResult = CompletableFuture.failedFuture(new IllegalStateException("broker unavailable"));
        server.unsubscribe(session, "topic");
        CompletableFuture<Void> lastSubscribe = server.subscribe(session, "topic").toCompletableFuture();
        firstUnsubscribe.complete(null);

        assertThat(lastSubscribe).as("Last subscribe").succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(broker.handlers).as("Broker handlers").hasSize(4);
        assertThat(broker.handlers.get(3)).as("The failed unsubscribe worked on the resubscribed handler").isSameAs(broker.handlers.get(2));

        broker.handlers.get(2).onMessage("topic", "delivered");
        channel.runPendingTasks();

        TextWebSocketFrame frame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(TextWebSocketFrame.class)).actual();
        assertThat(frame.text()).as("The handler the broker holds should deliver").isEqualTo("delivered");
        frame.release();
    }

    @Test
    void shouldDropHandlerWhenSubscribeSucceedsAfterTimeoutBehindQueuedOperations() {
        server.configure(configurer -> configurer.setBrokerOperationTimeout(Duration.ofMillis(100)));
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        CompletableFuture<Void> subscribing = new CompletableFuture<>();
        CompletableFuture<Void> publishing = new CompletableFuture<>();
        broker.subscribeResult = subscribing;
        broker.publishResult = publishing;

        // A publish and an unsubscribe queue up behind the pending subscribe
        CompletableFuture<Void> subscribe = server.subscribe(session, "topic").toCompletableFuture();
        server.publish("topic", "message");
        server.unsubscribe(session, "topic");

        assertThat(subscribe).as("Subscribe should time out").failsWithin(5, TimeUnit.SECONDS);

        subscribing.complete(null);

        assertThat(broker.calls).as("Broker calls").containsExactly("subscribe topic", "publish topic message", "unsubscribe topic");
        assertThat(broker.handlers.get(1)).as("The late handler should be unsubscribed").isSameAs(broker.handlers.getFirst());
        publishing.complete(null);
    }

    @Test
    void shouldDropHandlerWhenTimeoutContinuationCompletesBrokerSubscribe() {
        server.configure(configurer -> configurer.setBrokerOperationTimeout(Duration.ofMillis(100)));
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        CompletableFuture<Void> subscribing = new CompletableFuture<>();
        broker.subscribeResult = subscribing;

        // The application reacts to the timeout by completing the broker's own stage
        CompletableFuture<Void> subscribe = server.subscribe(session, "topic")
            .whenComplete((_, _) -> subscribing.complete(null))
            .toCompletableFuture();

        assertThat(subscribe).as("Subscribe should time out").failsWithin(5, TimeUnit.SECONDS);
        // The cleanup runs on the timeout thread, after the continuation that woke this thread
        awaitCalls(2);
        assertThat(broker.calls).as("Broker calls").containsExactly("subscribe topic", "unsubscribe topic");
    }

    @Test
    void shouldNotDeliverThroughHandlerOfTimedOutUnsubscribeWhenResubscribeInstalledIt() {
        server.configure(configurer -> configurer.setBrokerOperationTimeout(Duration.ofMillis(100)));
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        server.subscribe(session, "topic").toCompletableFuture().join();
        CompletableFuture<Void> firstUnsubscribe = new CompletableFuture<>();
        CompletableFuture<Void> publishing = new CompletableFuture<>();
        broker.unsubscribeResult = firstUnsubscribe;

        // Unsubscribe, subscribe, unsubscribe, publish, subscribe queue up while the first unsubscribe is pending,
        // the second unsubscribe works on the handler of the resubscribe and times out
        server.unsubscribe(session, "topic");
        server.subscribe(session, "topic");
        broker.unsubscribeResult = new CompletableFuture<>();
        CompletableFuture<Void> secondUnsubscribe = server.unsubscribe(session, "topic").toCompletableFuture();
        broker.publishResult = publishing;
        server.publish("topic", "message");
        server.subscribe(session, "topic");
        firstUnsubscribe.complete(null);

        assertThat(secondUnsubscribe).as("Second unsubscribe should time out").failsWithin(5, TimeUnit.SECONDS);
        assertThat(broker.handlers).as("Broker handlers").hasSize(4);

        broker.handlers.get(2).onMessage("topic", "stale");
        channel.runPendingTasks();

        assertThat(Util.readFromServer(channel)).as("The timed-out handler should deliver nothing").isNull();
        publishing.complete(null);
    }

    private void awaitCalls(int count) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);

        while (broker.calls.size() < count && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    @Test
    void shouldNotDeliverThroughHandlerOfTimedOutUnsubscribeWhileResubscribeWaits() {
        server.configure(configurer -> configurer.setBrokerOperationTimeout(Duration.ofMillis(100)));
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        server.subscribe(session, "topic").toCompletableFuture().join();
        TopicMessageHandler<String> handler = broker.handlers.getFirst();
        CompletableFuture<Void> publishing = new CompletableFuture<>();
        broker.unsubscribeResult = new CompletableFuture<>();
        broker.publishResult = publishing;

        // A publish and a new subscribe queue up behind the unsubscribe that times out
        CompletableFuture<Void> unsubscribe = server.unsubscribe(session, "topic").toCompletableFuture();
        server.publish("topic", "message");
        server.subscribe(session, "topic");

        assertThat(unsubscribe).as("Unsubscribe should time out").failsWithin(5, TimeUnit.SECONDS);

        handler.onMessage("topic", "stale");
        channel.runPendingTasks();

        assertThat(Util.readFromServer(channel)).as("The timed-out handler should deliver nothing").isNull();
        publishing.complete(null);
    }

    @Test
    @Timeout(10)
    void shouldDeliverToSubscribersOfItsOwnSubscriptionWhenHandlerIsRetiredDuringEncoding() throws Exception {
        CountDownLatch encoding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        server.configure(configurer -> configurer
            .setBrokerOperationTimeout(Duration.ofMillis(100))
            .setMessageEncoder(message -> {
                if (message.equals("old")) {
                    encoding.countDown();
                    awaitQuietly(release);
                }
                return message.getBytes(StandardCharsets.UTF_8);
            }));
        EmbeddedChannel first = Util.connect(server);
        server.subscribe(server.getSessionByChannelId(first.id()), "topic").toCompletableFuture().join();
        TopicMessageHandler<String> handler = broker.handlers.getFirst();

        // The old handler passes its check and blocks in the encoder while its subscription is replaced
        Thread delivery = Thread.ofPlatform().start(() -> handler.onMessage("topic", "old"));
        assertThat(encoding.await(5, TimeUnit.SECONDS)).isTrue();
        broker.unsubscribeResult = new CompletableFuture<>();
        assertThat(server.unsubscribe(server.getSessionByChannelId(first.id()), "topic").toCompletableFuture())
            .failsWithin(5, TimeUnit.SECONDS);
        EmbeddedChannel second = Util.connect(server);
        broker.subscribeResult = CompletableFuture.completedFuture(null);
        server.subscribe(server.getSessionByChannelId(second.id()), "topic").toCompletableFuture().join();
        release.countDown();
        delivery.join();
        second.runPendingTasks();

        assertThat(Util.readFromServer(second)).as("The new subscriber should not get the old message").isNull();
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
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
