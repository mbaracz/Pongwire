package pl.mbaracz.pongwire;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class IdleTimeoutTest {

    private record Close(int code, String reason) {
    }

    private final List<Close> closes = new ArrayList<>();
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                .setAllowBinaryFrames(true)
                .setIdleTimeout(Duration.ofSeconds(30))
            )
            .onClose((_, reason, code) -> closes.add(new Close(code, reason)));
    }

    @Test
    void shouldKeepIdleSessionOpenWhenIdleTimeoutIsDisabled() {
        server.configure(configurer -> configurer.setIdleTimeout(null));
        EmbeddedChannel channel = connect();

        advanceTime(channel, 3600);

        assertThat(channel.isOpen()).as("Session should stay open").isTrue();
        assertThat(Util.readFromServer(channel)).as("No close frame should be sent").isNull();
        assertThat(closes).isEmpty();
    }

    @Test
    void shouldCloseSessionWhenNoApplicationMessageArrivesBeforeIdleTimeout() {
        EmbeddedChannel channel = connect();

        advanceTime(channel, 29);
        assertThat(channel.isOpen()).as("Session should stay open before the timeout").isTrue();

        advanceTime(channel, 1);

        assertIdleTimeout(channel);
    }

    @Test
    void shouldResetIdleTimeoutWhenTextOrBinaryMessageArrives() {
        EmbeddedChannel channel = connect();

        advanceTime(channel, 20);
        Util.sendFromClient(channel, new TextWebSocketFrame("text"));
        advanceTime(channel, 20);
        Util.sendFromClient(channel, new BinaryWebSocketFrame(
            Unpooled.copiedBuffer("binary", StandardCharsets.UTF_8)
        ));
        advanceTime(channel, 29);

        assertThat(channel.isOpen()).as("Messages should reset the timeout").isTrue();
        assertThat(Util.readFromServer(channel)).as("No close frame should be sent yet").isNull();

        advanceTime(channel, 1);
        assertIdleTimeout(channel);
    }

    @Test
    void shouldRecordLastMessageTimeWhenMessageArrives() {
        EmbeddedChannel channel = connect();
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());
        assertThat(session.getLastMessageTime()).as("No message yet").isNull();
        Instant before = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        Util.sendFromClient(channel, new TextWebSocketFrame("text"));

        assertThat(session.getLastMessageTime()).as("Last message time").isBetween(before, Instant.now());
    }

    @Test
    void shouldNotResetIdleTimeoutWhenPongArrives() {
        EmbeddedChannel channel = connect();

        advanceTime(channel, 29);
        Util.sendFromClient(channel, new PongWebSocketFrame());
        advanceTime(channel, 1);

        assertIdleTimeout(channel);
    }

    private EmbeddedChannel connect() {
        EmbeddedChannel channel = Util.connect(server);
        channel.freezeTime();
        return channel;
    }

    private static void advanceTime(EmbeddedChannel channel, long seconds) {
        channel.advanceTimeBy(seconds, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
    }

    private void assertIdleTimeout(EmbeddedChannel channel) {
        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel))
            .asInstanceOf(type(CloseWebSocketFrame.class))
            .actual();
        assertThat(closeFrame.statusCode()).as("Should send going away status")
            .isEqualTo(WebSocketCloseStatus.ENDPOINT_UNAVAILABLE.code());
        assertThat(closeFrame.reasonText()).as("Should send idle timeout reason").isEqualTo("Idle timeout");
        assertThat(channel.isOpen()).as("Session should wait for the client's close frame").isTrue();
        closeFrame.release();

        Util.sendFromClient(channel, new CloseWebSocketFrame(WebSocketCloseStatus.NORMAL_CLOSURE));

        assertThat(channel.isOpen()).as("Session should be closed after the client's answer").isFalse();
        assertThat(closes).as("Close handler should be called once")
            .isEqualTo(List.of(new Close(1001, "Idle timeout")));
    }
}
