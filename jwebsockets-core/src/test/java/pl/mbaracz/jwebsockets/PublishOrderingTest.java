package pl.mbaracz.jwebsockets;

import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class PublishOrderingTest {

    // Both topics have the same hash code, so the in-memory broker delivers them one after another
    private static final String SLOW_TOPIC = "Aa";
    private static final String QUEUED_TOPIC = "BB";

    private final CountDownLatch encoding = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(message -> {
                    if (message.equals("slow")) {
                        encoding.countDown();
                        awaitRelease();
                    }
                    return message.getBytes(StandardCharsets.UTF_8);
                })
            )
            .listen(0);
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    private void awaitRelease() {
        try {
            assertThat(release.await(5, TimeUnit.SECONDS)).as("Encoder should be released").isTrue();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private EmbeddedChannel connect() {
        EmbeddedChannel channel = Util.newEmbeddedChannel(DefaultChannelId.newInstance(), new WebSocketServerChannelInitializer<>(server));
        Util.performHandshake(channel, "/");
        channel.releaseOutbound();
        return channel;
    }

    @Test
    void shouldNotDeliverMessagePublishedBeforeSubscribingWhenDeliveryOfTopicIsQueued() throws Exception {
        EmbeddedChannel slowSubscriber = connect();
        EmbeddedChannel lateSubscriber = connect();
        server.subscribe(server.getSessionByChannelId(slowSubscriber.id()), SLOW_TOPIC);

        Thread publisher = Thread.ofPlatform().start(() -> server.publish(SLOW_TOPIC, "slow"));
        assertThat(encoding.await(5, TimeUnit.SECONDS)).as("Delivery should start").isTrue();

        // The message waits for the slow delivery, and the subscription comes after it
        server.publish(QUEUED_TOPIC, "early");
        CompletableFuture<Void> subscribe = server.subscribe(server.getSessionByChannelId(lateSubscriber.id()), QUEUED_TOPIC).toCompletableFuture();
        release.countDown();
        publisher.join();
        subscribe.get(1, TimeUnit.SECONDS);
        slowSubscriber.runPendingTasks();
        lateSubscriber.runPendingTasks();

        TextWebSocketFrame frame = assertThat(Util.readFromServer(slowSubscriber)).asInstanceOf(type(TextWebSocketFrame.class)).actual();
        assertThat(frame.text()).as("Slow topic message").isEqualTo("slow");
        frame.release();
        assertThat(Util.readFromServer(lateSubscriber)).as("Message published before subscribing").isNull();
    }

    @Test
    void shouldCountConnectedSessions() {
        EmbeddedChannel first = connect();
        connect();

        assertThat(server.getSessionCount()).as("Sessions after connecting").isEqualTo(2);

        first.close();

        assertThat(server.getSessionCount()).as("Sessions after disconnecting").isEqualTo(1);
    }
}
