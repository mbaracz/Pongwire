package pl.mbaracz.pongwire;

import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class PubSubTest {

    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            )
            .listen(8085);
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    @Test
    void shouldReceiveMessageWhenUserIsSubscribedAndMessageIsPublished() {
        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Get session from channel id
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());

        // Assert session is not null
        assertThat(session).as("Session should not be null").isNotNull();

        // Subscribe to topic
        String topic = "topic-1";
        server.subscribe(session, topic);

        // Publish message
        String message = "topic-1-message";
        server.publish(topic, message);
        channel.runPendingTasks();

        // Read outgoing frame
        TextWebSocketFrame textWebSocketFrame = channel.readOutbound();
        String outputMessage = textWebSocketFrame.text();

        // Assert messages are equal
        assertThat(outputMessage).as("Received message should be equal to sent").isEqualTo(message);
        textWebSocketFrame.release();
    }

    @Test
    void shouldNotReceiveMessageWhenUserIsNotSubscribedAndMessageIsPublished() {
        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Get session from channel id
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());

        // Assert session is not null
        assertThat(session).as("Session should not be null").isNotNull();

        // Publish message
        String topic = "topic-2";
        String message = "topic-2-message";
        server.publish(topic, message);
        channel.runPendingTasks();

        // Assert outgoing frame is null
        assertThat(channel.<Object>readOutbound()).as("Outgoing frame should be null").isNull();
    }

    @Test
    void shouldNotReceiveMessageWhenUserUnsubscribedTopic() {
        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Get session from channel id
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());

        // Assert session is not null
        assertThat(session).as("Session should not be null").isNotNull();

        // Subscribe to topic
        String topic = "topic-2";
        server.subscribe(session, topic);

        // Publish message
        String message = "topic-2-message";
        server.publish(topic, message);
        channel.runPendingTasks();

        // Assert outgoing frame is null
        TextWebSocketFrame textWebSocketFrame = channel.readOutbound();
        assertThat(textWebSocketFrame).as("Outgoing frame should not be null").isNotNull();
        textWebSocketFrame.release();

        // Unsubscribe and publish again
        server.unsubscribe(session, topic);
        server.publish(topic, message);
        channel.runPendingTasks();

        // Assert outgoing frame is null
        assertThat(channel.<Object>readOutbound()).as("Outgoing frame should be null").isNull();
    }

    @Test
    void shouldRemoveTopicWhenAllUsersUnsubscribedIt() {
        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Get session from channel id
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());

        // Assert session is not null
        assertThat(session).as("Session should not be null").isNotNull();

        // Subscribe to topic
        String topic = "topic-2";
        server.subscribe(session, topic);

        // Assert that topic was registered
        assertThat(server.getTopics()).as("List of topics should not be empty").isNotEmpty();

        // Unsubscribe topic
        server.unsubscribe(session, topic);

        // Assert that topic was removed
        assertThat(server.getTopics()).as("List of topics should be empty").isEmpty();
    }

    @Test
    void shouldUnsubscribeUserWhenItDisconnects() {
        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Get session from channel id
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());

        // Assert session is not null
        assertThat(session).as("Session should not be null").isNotNull();

        // Subscribe to topic
        String topic = "topic-test";
        server.subscribe(session, topic);

        // Assert user is subscribed
        assertThat(server.isSubscribed(session, topic)).isTrue();

        // Close connection
        channel.writeInbound(new CloseWebSocketFrame());

        // Assert session was unsubscribed and the empty topic was removed
        assertThat(server.isSubscribed(session, topic)).as("Session should be unsubscribed").isFalse();
        assertThat(server.getTopics()).as("Topic should be removed").doesNotContain(topic);
    }

    @Test
    void shouldRemoveOnlyEmptyTopicsWhenUserDisconnects() {
        // Construct channels and perform handshakes, EmbeddedChannel instances share the same id by default
        EmbeddedChannel firstChannel = Util.newEmbeddedChannel(DefaultChannelId.newInstance(), new WebSocketServerHandler<>(server));
        EmbeddedChannel secondChannel = Util.newEmbeddedChannel(DefaultChannelId.newInstance(), new WebSocketServerHandler<>(server));
        Util.completeHandshake(firstChannel, "/");
        Util.completeHandshake(secondChannel, "/");

        // Get sessions from channel ids
        WebSocketSession<String, Object> firstSession = server.getSessionByChannelId(firstChannel.id());
        WebSocketSession<String, Object> secondSession = server.getSessionByChannelId(secondChannel.id());

        // Subscribe both sessions to a shared topic and the first one also to its own topic
        String sharedTopic = "topic-shared";
        String ownTopic = "topic-own";
        server.subscribe(firstSession, sharedTopic);
        server.subscribe(secondSession, sharedTopic);
        server.subscribe(firstSession, ownTopic);

        // Close first connection
        firstChannel.writeInbound(new CloseWebSocketFrame());

        // Assert first session left all topics, its own topic was removed and the shared one was kept
        assertThat(server.isSubscribed(firstSession, sharedTopic)).as("First session should be unsubscribed").isFalse();
        assertThat(server.getTopics()).as("Topic without subscribers should be removed").doesNotContain(ownTopic);
        assertThat(server.isSubscribed(secondSession, sharedTopic)).as("Second session should stay subscribed").isTrue();
    }

    @Test
    void shouldQueueTopicMessageWhenPublishedOnSubscribersEventLoop() {
        EmbeddedChannel channel = Util.connect(server);
        server.subscribe(server.getSessionByChannelId(channel.id()), "topic");

        // An embedded channel treats every thread as its event loop, like a message handler publishing. Writing
        // right away would overtake deliveries other threads queued on the loop before, out of the broker order.
        server.publish("topic", "message");

        assertThat(channel.<Object>readOutbound()).as("Topic message should wait for the event loop").isNull();

        channel.runPendingTasks();

        assertThat(Util.readFromServer(channel))
            .as("Topic message should be delivered by the event loop")
            .asInstanceOf(type(TextWebSocketFrame.class))
            .satisfies(frame -> assertThat(frame.text()).isEqualTo("message"))
            .satisfies(TextWebSocketFrame::release);
    }

    @Test
    void shouldDeliverTopicMessageToSubscribersOnEveryEventLoop() {
        EmbeddedChannel firstChannel = Util.newEmbeddedChannel(DefaultChannelId.newInstance(), new WebSocketServerChannelInitializer<>(server));
        EmbeddedChannel secondChannel = Util.newEmbeddedChannel(DefaultChannelId.newInstance(), new WebSocketServerChannelInitializer<>(server));
        Util.performHandshake(firstChannel, "/");
        Util.performHandshake(secondChannel, "/");
        firstChannel.releaseOutbound();
        secondChannel.releaseOutbound();
        server.subscribe(server.getSessionByChannelId(firstChannel.id()), "topic");
        server.subscribe(server.getSessionByChannelId(secondChannel.id()), "topic");

        server.publish("topic", "message");
        firstChannel.runPendingTasks();
        secondChannel.runPendingTasks();

        for (EmbeddedChannel channel : List.of(firstChannel, secondChannel)) {
            assertThat(Util.readFromServer(channel))
                .as("Every subscriber should receive the message")
                .asInstanceOf(type(TextWebSocketFrame.class))
                .satisfies(frame -> assertThat(frame.text()).isEqualTo("message"))
                .satisfies(TextWebSocketFrame::release);
        }
    }
}
