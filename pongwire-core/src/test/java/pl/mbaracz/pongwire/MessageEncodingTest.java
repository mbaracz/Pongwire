package pl.mbaracz.pongwire;

import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class MessageEncodingTest {

    private final AtomicInteger encodings = new AtomicInteger();
    private final List<Throwable> errors = new ArrayList<>();
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(message -> {
                    encodings.incrementAndGet();

                    return switch (message) {
                        case "invalid" -> new byte[]{(byte) 0xC3, (byte) 0x28};
                        case "missing" -> null;
                        case "failing" -> throw new IllegalStateException("encoder failed");
                        default -> message.getBytes(StandardCharsets.UTF_8);
                    };
                })
            )
            .onError((_, exception) -> errors.add(exception))
            .listen(0);
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    private EmbeddedChannel connect() {
        EmbeddedChannel channel = Util.newEmbeddedChannel(DefaultChannelId.newInstance(), new WebSocketServerChannelInitializer<>(server));
        Util.performHandshake(channel, "/");
        channel.releaseOutbound();
        return channel;
    }

    private static void assertReceived(EmbeddedChannel channel, String text) {
        TextWebSocketFrame frame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(TextWebSocketFrame.class)).actual();
        assertThat(frame.text()).as("Received text").isEqualTo(text);
        frame.release();
    }

    @Test
    void shouldEncodeBroadcastOnceForEverySession() {
        EmbeddedChannel first = connect();
        EmbeddedChannel second = connect();

        server.broadcast("zażółć");

        assertThat(encodings).as("Message should be encoded once").hasValue(1);
        assertReceived(first, "zażółć");
        assertReceived(second, "zażółć");
    }

    @Test
    void shouldEncodeTopicMessageOnceForEverySubscriber() {
        EmbeddedChannel first = connect();
        EmbeddedChannel second = connect();
        server.subscribe(server.getSessionByChannelId(first.id()), "topic");
        server.subscribe(server.getSessionByChannelId(second.id()), "topic");

        server.publish("topic", "news").toCompletableFuture().join();
        first.runPendingTasks();
        second.runPendingTasks();

        assertThat(encodings).as("Message should be encoded once").hasValue(1);
        assertReceived(first, "news");
        assertReceived(second, "news");
    }

    @Test
    void shouldReportBroadcastEncodingFailureToCallerOnce() {
        connect();
        connect();

        assertThatThrownBy(() -> server.broadcast("failing")).hasMessage("encoder failed");
        assertThat(errors).as("Sessions should not report the failure").isEmpty();
    }

    @Test
    void shouldFailPublishWhenTopicMessageCannotBeEncoded() {
        EmbeddedChannel channel = connect();
        server.subscribe(server.getSessionByChannelId(channel.id()), "topic");

        CompletableFuture<Void> publish = server.publish("topic", "failing").toCompletableFuture();

        assertThat(publish).as("Publish should fail").isCompletedExceptionally();
        assertThat(errors).as("Subscribers should not report the failure").isEmpty();
    }

    @Test
    void shouldFailSendWhenEncoderReturnsInvalidUtf8ForTextFrame() {
        EmbeddedChannel channel = connect();

        CompletableFuture<Void> send = server.getSessionByChannelId(channel.id()).sendMessageAsync("invalid").toCompletableFuture();

        assertThatThrownBy(send::join).cause().isInstanceOf(IllegalArgumentException.class).hasMessageContaining("UTF-8");
    }

    @Test
    void shouldFailSendWhenEncoderReturnsNull() {
        EmbeddedChannel channel = connect();

        CompletableFuture<Void> send = server.getSessionByChannelId(channel.id()).sendMessageAsync("missing").toCompletableFuture();

        assertThatThrownBy(send::join).cause().isInstanceOf(NullPointerException.class).hasMessageContaining("encoder returned null");
    }

    @Test
    void shouldRejectNullMessagesRightAway() {
        WebSocketSession<String, Object> session = server.getSessionByChannelId(connect().id());

        assertThatThrownBy(() -> session.sendMessage(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> session.sendMessageAsync(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> server.broadcast(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> server.publish("topic", null)).isInstanceOf(NullPointerException.class);
        assertThat(encodings).as("Nothing should be encoded").hasValue(0);
    }
}
