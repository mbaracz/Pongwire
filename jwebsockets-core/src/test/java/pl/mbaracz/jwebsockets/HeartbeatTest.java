package pl.mbaracz.jwebsockets;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class HeartbeatTest {

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
                .setHeartbeatInterval(Duration.ofSeconds(30))
                .setHeartbeatTimeout(Duration.ofSeconds(10))
            )
            .onClose((_, reason, code) -> closes.add(new Close(code, reason)));
    }

    /**
     * Connects a client and stops the clock of its channel, so the test decides when idle timers expire.
     */
    private EmbeddedChannel connect() {
        EmbeddedChannel channel = Util.connect(server);
        channel.freezeTime();
        return channel;
    }

    private static void advanceTime(EmbeddedChannel channel, long seconds) {
        channel.advanceTimeBy(seconds, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
    }

    private static void assertPingSent(EmbeddedChannel channel) {
        PingWebSocketFrame ping = assertThat(Util.readFromServer(channel))
            .as("Should send a ping")
            .asInstanceOf(type(PingWebSocketFrame.class))
            .actual();
        ping.release();
    }

    @Test
    void shouldNotSendPingWhenHeartbeatIsDisabled() {
        server.configure(configurer -> configurer.setHeartbeatInterval(null));

        EmbeddedChannel channel = connect();

        advanceTime(channel, 3600);

        assertThat(Util.readFromServer(channel)).as("No ping should be sent").isNull();
        assertThat(channel.isOpen()).as("Channel should stay open").isTrue();
    }

    @Test
    void shouldSendPingWhenSessionIsIdle() {
        EmbeddedChannel channel = connect();

        advanceTime(channel, 30);

        assertPingSent(channel);
        assertThat(channel.isOpen()).as("Channel should stay open").isTrue();
    }

    @Test
    void shouldNotSendPingWhenChannelIsUnwritable() {
        server.configure(configurer -> configurer.setWriteBufferWaterMark(1024, 2048));
        EmbeddedChannel channel = connect();

        // A client reading slowly leaves the write buffer full, the pong could not come back in time
        channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void flush(ChannelHandlerContext context) {
            }
        });
        channel.writeAndFlush(new TextWebSocketFrame("x".repeat(4096)));
        long bufferedBytes = channel.unsafe().outboundBuffer().totalPendingWriteBytes();

        advanceTime(channel, 30);
        advanceTime(channel, 10);

        assertThat(channel.unsafe().outboundBuffer().totalPendingWriteBytes()).as("No ping should be queued").isEqualTo(bufferedBytes);
        assertThat(channel.isOpen()).as("Channel should stay open").isTrue();
        assertThat(closes).as("Session should not be closed").isEmpty();
    }

    @Test
    void shouldKeepConnectionOpenWhenPongArrivesInTime() {
        EmbeddedChannel channel = connect();

        advanceTime(channel, 30);
        assertPingSent(channel);

        // Answer the ping within the timeout, then wait past the moment the session would have been closed
        advanceTime(channel, 5);
        Util.sendFromClient(channel, new PongWebSocketFrame());
        advanceTime(channel, 10);

        assertThat(channel.isOpen()).as("Channel should stay open").isTrue();
        assertThat(closes).as("Session should not be closed").isEmpty();
    }

    @Test
    void shouldCloseSessionOnceWhenPongDoesNotArrive() {
        EmbeddedChannel channel = connect();

        advanceTime(channel, 30);
        assertPingSent(channel);

        // Let the timeout pass without a pong
        advanceTime(channel, 10);

        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send going away status").isEqualTo(WebSocketCloseStatus.ENDPOINT_UNAVAILABLE.code());
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(closes).as("Close handler should be called once").isEqualTo(List.of(new Close(1001, "Heartbeat timeout")));
        closeFrame.release();
    }

    @Test
    void shouldStillTimeOutSessionWhenDataArrivesButPongDoesNot() {
        EmbeddedChannel channel = connect();

        advanceTime(channel, 30);
        assertPingSent(channel);

        // Send a message instead of the pong, it must not count as an answer to the ping
        advanceTime(channel, 5);
        Util.sendFromClient(channel, new TextWebSocketFrame("Hello"));
        advanceTime(channel, 5);

        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(closes).as("Close handler should be called once").isEqualTo(List.of(new Close(1001, "Heartbeat timeout")));
    }
}
