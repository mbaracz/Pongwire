package pl.mbaracz.pongwire;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class SessionWritabilityTest {

    private record Close(int code, String reason) {
    }

    private final List<WebSocketSession<String, Object>> opened = new ArrayList<>();
    private final List<Close> closes = new ArrayList<>();
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            )
            .onOpen(opened::add)
            .onClose((_, reason, code) -> closes.add(new Close(code, reason)));
    }

    /**
     * Holds back flushes like a client that stops reading, so written messages stay in the write buffer.
     */
    private static void holdFlushes(EmbeddedChannel channel) {
        channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void flush(ChannelHandlerContext context) {
            }
        });
    }

    /**
     * Configures the watermarks and the unwritable timeout, then connects a client that stops reading
     * and writes a message above the high watermark to it, with the clock of the channel stopped,
     * so the test decides when timeouts expire.
     */
    private EmbeddedChannel connectSlowClient(Duration unwritableTimeout) {
        server.configure(configurer -> configurer
            .setWriteBufferWaterMark(1024, 2048)
            .setUnwritableTimeout(unwritableTimeout)
        );

        EmbeddedChannel channel = Util.connect(server);
        channel.freezeTime();
        holdFlushes(channel);
        channel.writeAndFlush(new TextWebSocketFrame("x".repeat(4096)));
        return channel;
    }

    private static void advanceTime(EmbeddedChannel channel, long seconds) {
        channel.advanceTimeBy(seconds, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
    }

    @Test
    void shouldBeWritableInitiallyWhenSessionIsConnected() {
        EmbeddedChannel channel = Util.connect(server);

        assertThat(opened.getFirst().isWritable()).as("Session should be writable").isTrue();

        channel.close();
    }

    @Test
    void shouldNotBeWritableUntilFlushedWhenWriteBufferExceedsHighWaterMark() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = opened.getFirst();

        channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(1024, 2048));
        holdFlushes(channel);

        session.sendMessage("x".repeat(4096));

        assertThat(channel.isOpen()).as("Channel should stay open").isTrue();
        assertThat(session.isWritable()).as("Session should not be writable above the high water mark").isFalse();

        // Flush the buffered message
        channel.pipeline().removeFirst();
        channel.flush();

        assertThat(session.isWritable()).as("Session should be writable again below the low water mark").isTrue();

        channel.close();
    }

    @Test
    void shouldNotifyWritabilityHandlerWhenWriteBufferCrossesWaterMarks() {
        List<Boolean> notifications = new ArrayList<>();

        server.onWritabilityChanged((_, writable) -> notifications.add(writable));

        EmbeddedChannel channel = Util.connect(server);
        channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(1024, 2048));
        holdFlushes(channel);

        opened.getFirst().sendMessage("x".repeat(4096));

        assertThat(notifications).as("Should be notified when the high water mark is exceeded").isEqualTo(List.of(false));

        // Flush the buffered message
        channel.pipeline().removeFirst();
        channel.flush();

        assertThat(notifications).as("Should be notified when the buffer drops below the low water mark").isEqualTo(List.of(false, true));

        channel.close();
    }

    @Test
    void shouldNotBeWritableWhenChannelIsClosed() {
        EmbeddedChannel channel = Util.connect(server);

        // Close the connection
        channel.close();

        assertThat(opened.getFirst().isWritable()).as("Session should not be writable").isFalse();
    }

    @Test
    void shouldNotCloseSessionImmediatelyWhenWriteBufferExceedsConfiguredHighWaterMark() {
        EmbeddedChannel channel = connectSlowClient(Duration.ofSeconds(10));

        advanceTime(channel, 5);

        assertThat(channel.isWritable()).as("Channel should not be writable above the configured high watermark").isFalse();
        assertThat(channel.isOpen()).as("Channel should stay open before the timeout").isTrue();
        assertThat(closes).as("Session should not be closed").isEmpty();
    }

    @Test
    void shouldNotCloseSessionWhenChannelBecomesWritableBeforeTimeout() {
        EmbeddedChannel channel = connectSlowClient(Duration.ofSeconds(10));

        advanceTime(channel, 5);

        // Flush the buffered message, which brings the buffer below the low watermark
        channel.pipeline().removeFirst();
        channel.flush();
        advanceTime(channel, 10);

        assertThat(channel.isWritable()).as("Channel should be writable again").isTrue();
        assertThat(channel.isOpen()).as("Channel should stay open").isTrue();
        assertThat(closes).as("Session should not be closed").isEmpty();
    }

    @Test
    void shouldCloseSessionOnceWhenChannelStaysUnwritableUntilTimeout() {
        EmbeddedChannel channel = connectSlowClient(Duration.ofSeconds(10));

        advanceTime(channel, 10);

        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(closes).as("Close handler should be called once").isEqualTo(List.of(new Close(1006, "Unwritable timeout")));
    }

    @Test
    void shouldKeepUnwritableSessionOpenWhenUnwritableTimeoutIsNotConfigured() {
        EmbeddedChannel channel = connectSlowClient(null);

        advanceTime(channel, 3600);

        assertThat(channel.isWritable()).as("Channel should stay unwritable").isFalse();
        assertThat(channel.isOpen()).as("Channel should stay open").isTrue();
        assertThat(closes).as("Session should not be closed").isEmpty();
    }

    @Test
    void shouldStillStartUnwritableTimeoutWhenChannelIsAlreadyUnwritableAsSessionOpens() {
        server.configure(configurer -> configurer
            .setWriteBufferWaterMark(1024, 2048)
            .setUnwritableTimeout(Duration.ofSeconds(10))
        );

        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));
        channel.freezeTime();

        // Make the channel unwritable before the handshake, so no writability change follows the opening of the session
        channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
        channel.runPendingTasks();
        Util.performHandshake(channel, "/");

        advanceTime(channel, 10);

        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(closes).as("Close handler should be called once").isEqualTo(List.of(new Close(1006, "Unwritable timeout")));
    }

    @Test
    void shouldRestartUnwritableTimeoutWhenWriteBufferDrains() {
        server.configure(configurer -> configurer
            .setWriteBufferWaterMark(1024, 2048)
            .setUnwritableTimeout(Duration.ofSeconds(10))
        );
        EmbeddedChannel channel = Util.connect(server);
        channel.freezeTime();

        AtomicBoolean holding = new AtomicBoolean(true);
        channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void flush(ChannelHandlerContext context) {
                if (!holding.get()) {
                    context.flush();
                }
            }
        });
        channel.writeAndFlush(new TextWebSocketFrame("x".repeat(4096)));

        // The buffered message reaches the client, while the connection stays unwritable for another reason
        channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
        holding.set(false);
        channel.flush();
        channel.releaseOutbound();

        advanceTime(channel, 10);
        assertThat(channel.isOpen()).as("Drained buffer should start a new timeout").isTrue();

        advanceTime(channel, 10);
        assertThat(channel.isOpen()).as("Buffer without progress should close the connection").isFalse();
    }

    @Test
    void shouldRestartUnwritableTimeoutWhenTransportTakesPartOfTheCurrentMessage() {
        EmbeddedChannel channel = connectSlowClient(Duration.ofSeconds(10));
        ChannelOutboundBuffer buffer = channel.unsafe().outboundBuffer();

        // The message is flushed to the transport, which takes it in parts over several timeouts
        buffer.addFlush();
        advanceTime(channel, 5);
        buffer.progress(1000);
        advanceTime(channel, 5);
        assertThat(channel.isOpen()).as("A partly written message should start a new timeout").isTrue();

        advanceTime(channel, 5);
        buffer.progress(1000);
        advanceTime(channel, 5);
        assertThat(channel.isOpen()).as("Further progress should start another timeout").isTrue();

        advanceTime(channel, 10);
        assertThat(channel.isOpen()).as("A message without progress should close the connection").isFalse();
    }
}
