package pl.mbaracz.pongwire;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class SessionPubSubConcurrencyTest {

    private static final String TOPIC = "topic";

    private interface PubSubOperation {
        CompletionStage<Void> run(
            WebSocketServer<String, Object> server,
            WebSocketSession<String, Object> session
        );
    }

    private static final class PausingTopicBroker implements TopicBroker<String> {

        private final CountDownLatch unsubscribeStarted = new CountDownLatch(1);
        private final CompletableFuture<Void> continueUnsubscribe = new CompletableFuture<>();

        @Override
        public CompletionStage<Void> subscribe(String topic, TopicMessageHandler<String> handler) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> unsubscribe(String topic, TopicMessageHandler<String> handler) {
            unsubscribeStarted.countDown();
            return continueUnsubscribe;
        }

        @Override
        public CompletionStage<Void> publish(String topic, String message) {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Test
    @Timeout(10)
    void shouldNotRestoreSubscriptionWhenSubscribeRacesWithDisconnect() throws Exception {
        Throwable failure = runRace((server, session) -> server.subscribe(session, TOPIC));

        assertThat(failure)
            .as("Subscribe should reject the session once disconnect has removed it")
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("WebSocket session is not connected");
    }

    @Test
    @Timeout(10)
    void shouldKeepRegistriesEmptyWhenUnsubscribeRacesWithDisconnect() throws Exception {
        assertThat(runRace((server, session) -> server.unsubscribe(session, TOPIC))).isNull();
    }

    @Test
    @Timeout(10)
    void shouldKeepRegistriesEmptyWhenPublishRacesWithDisconnect() throws Exception {
        assertThat(runRace((server, _) -> server.publish(TOPIC, "message"))).isNull();
    }

    private static Throwable runRace(PubSubOperation operation) throws Exception {
        PausingTopicBroker broker = new PausingTopicBroker();
        WebSocketServer<String, Object> server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            )
            .topicBroker(broker);
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        server.subscribe(session, TOPIC).toCompletableFuture().join();

        AtomicReference<Throwable> disconnectFailure = new AtomicReference<>();
        Thread disconnect = Thread.ofPlatform().start(() -> {
            try {
                channel.close();
            } catch (Throwable throwable) {
                disconnectFailure.set(throwable);
            }
        });

        assertThat(broker.unsubscribeStarted.await(5, TimeUnit.SECONDS))
            .as("Disconnect should reach broker cleanup")
            .isTrue();

        AtomicReference<Throwable> operationFailure = new AtomicReference<>();
        AtomicReference<CompletionStage<Void>> result = new AtomicReference<>();
        Thread concurrentOperation = Thread.ofPlatform().start(() -> {
            try {
                result.set(operation.run(server, session));
            } catch (Throwable throwable) {
                operationFailure.set(throwable);
            }
        });

        concurrentOperation.join();
        broker.continueUnsubscribe.complete(null);
        disconnect.join();

        if (result.get() != null) {
            try {
                result.get().toCompletableFuture().join();
            } catch (CompletionException exception) {
                operationFailure.compareAndSet(null, exception.getCause());
            }
        }

        assertThat(disconnectFailure.get()).isNull();
        assertThat(server.getConnectedSessions()).as("Session registry should be empty").isEmpty();
        assertThat(server.isSubscribed(session, TOPIC)).as("Disconnected session should not remain subscribed").isFalse();
        assertThat(server.getTopics()).as("Empty topic should be removed").doesNotContain(TOPIC);

        return operationFailure.get();
    }
}
