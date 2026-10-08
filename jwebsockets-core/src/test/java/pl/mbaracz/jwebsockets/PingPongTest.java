package pl.mbaracz.jwebsockets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocket13FrameDecoder;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class PingPongTest {

    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            );
    }

    /**
     * Holds back flushes like a client that stops reading, until the gate is opened.
     */
    private static final class OutboundGate extends ChannelOutboundHandlerAdapter {

        private boolean open;

        @Override
        public void flush(ChannelHandlerContext context) {
            if (open) {
                context.flush();
            }
        }

        void open(EmbeddedChannel channel) {
            open = true;
            channel.flush();
        }
    }

    private static OutboundGate holdFlushes(EmbeddedChannel channel) {
        OutboundGate gate = new OutboundGate();
        channel.pipeline().addFirst(gate);
        return gate;
    }

    private static AtomicInteger countFlushes(EmbeddedChannel channel) {
        AtomicInteger flushes = new AtomicInteger();
        channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void flush(ChannelHandlerContext context) {
                flushes.incrementAndGet();
                context.flush();
            }
        });
        return flushes;
    }

    /**
     * Decodes every frame the server has written so far.
     */
    private static List<WebSocketFrame> readAllFromServer(EmbeddedChannel channel) {
        EmbeddedChannel client = new EmbeddedChannel(new WebSocket13FrameDecoder(false, true, 65536));
        List<WebSocketFrame> frames = new ArrayList<>();
        ByteBuf buffer;
        WebSocketFrame frame;

        while ((buffer = channel.readOutbound()) != null) {
            client.writeInbound(buffer);
        }

        while ((frame = client.readInbound()) != null) {
            frames.add(frame);
        }

        client.finishAndReleaseAll();
        return frames;
    }

    @AfterEach
    void tearDown() {
        if (server.isRunning()) {
            server.stop();
        }
    }

    @Test
    void shouldRespondWithPongWhenClientSendsPing() {
        server.listen(8083);

        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Construct ping frame and send it, the pong is flushed when the read completes
        String messageToSend = "heartbeat";
        ByteBuf byteBuf = Unpooled.wrappedBuffer(messageToSend.getBytes(StandardCharsets.UTF_8));
        PingWebSocketFrame pingFrame = new PingWebSocketFrame(byteBuf);
        channel.writeInbound(pingFrame);

        // Read pong frame
        PongWebSocketFrame pongFrame = channel.readOutbound();
        String outputMessage = pongFrame.content().toString(StandardCharsets.UTF_8);

        // Assert we received pong frame with the same content
        assertThat(outputMessage).as("Received text differs from the sent one").isEqualTo(messageToSend);
        pongFrame.release();
    }

    @Test
    void shouldIgnorePongWhenClientSendsIt() {
        server
            .configure(configurer -> configurer.setCloseOnException(true))
            .listen(8084);

        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Construct pong frame and send
        ByteBuf byteBuf = Unpooled.wrappedBuffer("heartbeat".getBytes(StandardCharsets.UTF_8));
        channel.writeInbound(new PongWebSocketFrame(byteBuf));

        // Assert pong is accepted without a response and without closing the connection
        assertThat(channel.<Object>readOutbound()).as("Outbound should be null").isNull();
        assertThat(channel.isOpen()).as("Channel should stay open").isTrue();
    }

    @Test
    void shouldFlushPongsOnceWhenPingsArriveInOneRead() {
        EmbeddedChannel channel = Util.connect(server);
        AtomicInteger flushes = countFlushes(channel);

        Util.sendFromClient(channel, new PingWebSocketFrame(), new PingWebSocketFrame(), new PingWebSocketFrame());

        List<WebSocketFrame> frames = readAllFromServer(channel);

        assertThat(flushes.get()).as("Should flush once per read").isEqualTo(1);
        assertThat(frames).as("Should answer every Ping").hasSize(3).allMatch(PongWebSocketFrame.class::isInstance);
        frames.forEach(WebSocketFrame::release);
    }

    @Test
    void shouldNotQueuePongsWhenChannelIsUnwritable() {
        server.configure(configurer -> configurer.setWriteBufferWaterMark(1024, 2048));
        EmbeddedChannel channel = Util.connect(server);
        holdFlushes(channel);
        channel.writeAndFlush(new TextWebSocketFrame("x".repeat(4096)));
        long bufferedBytes = channel.unsafe().outboundBuffer().totalPendingWriteBytes();

        for (int i = 0; i < 100; i++) {
            Util.sendFromClient(channel, new PingWebSocketFrame(Unpooled.copiedBuffer("ping " + i, StandardCharsets.UTF_8)));
        }

        assertThat(channel.unsafe().outboundBuffer().totalPendingWriteBytes())
            .as("Pongs should not grow the write buffer")
            .isEqualTo(bufferedBytes);
    }

    @Test
    void shouldAnswerMostRecentPingWhenChannelBecomesWritableAgain() {
        server.configure(configurer -> configurer.setWriteBufferWaterMark(1024, 2048));
        EmbeddedChannel channel = Util.connect(server);
        OutboundGate gate = holdFlushes(channel);
        channel.writeAndFlush(new TextWebSocketFrame("x".repeat(4096)));

        Util.sendFromClient(channel, new PingWebSocketFrame(Unpooled.copiedBuffer("first", StandardCharsets.UTF_8)));
        Util.sendFromClient(channel, new PingWebSocketFrame(Unpooled.copiedBuffer("latest", StandardCharsets.UTF_8)));
        gate.open(channel);

        List<WebSocketFrame> frames = readAllFromServer(channel);

        assertThat(frames).as("Should send the text and one Pong").hasSize(2);
        assertThat(frames.get(1)).as("Should answer the most recent Ping").isInstanceOf(PongWebSocketFrame.class);
        assertThat(frames.get(1).content().toString(StandardCharsets.UTF_8)).as("Pong payload").isEqualTo("latest");
        frames.forEach(WebSocketFrame::release);
    }

    @Test
    void shouldAnswerDeferredPingBeforeWritabilityHandlerFillsBufferAgain() {
        AtomicBoolean refilled = new AtomicBoolean();
        server
            .configure(configurer -> configurer.setWriteBufferWaterMark(1024, 2048))
            .onWritabilityChanged((session, writable) -> {
                // Fills the buffer again once, like a sender resuming after backpressure
                if (writable && refilled.compareAndSet(false, true)) {
                    session.sendMessage("y".repeat(4096));
                }
            });
        EmbeddedChannel channel = Util.connect(server);
        OutboundGate gate = holdFlushes(channel);
        channel.writeAndFlush(new TextWebSocketFrame("x".repeat(4096)));

        Util.sendFromClient(channel, new PingWebSocketFrame(Unpooled.copiedBuffer("ping", StandardCharsets.UTF_8)));
        gate.open(channel);

        List<WebSocketFrame> frames = readAllFromServer(channel);

        assertThat(frames).as("Should send the text, the Pong and the refill").hasSize(3);
        assertThat(frames.get(1)).as("Pong should precede the refill").isInstanceOf(PongWebSocketFrame.class);
        frames.forEach(WebSocketFrame::release);
    }

    @Test
    void shouldSendDeferredPongOnceWhenItsWriteChangesWritability() {
        // Every pending byte makes the channel unwritable, so writing the Pong changes the writability again
        server.configure(configurer -> configurer.setWriteBufferWaterMark(1, 1));
        EmbeddedChannel channel = Util.connect(server);
        OutboundGate gate = holdFlushes(channel);
        channel.writeAndFlush(new TextWebSocketFrame("text"));

        Util.sendFromClient(channel, new PingWebSocketFrame(Unpooled.copiedBuffer("ping", StandardCharsets.UTF_8)));
        gate.open(channel);

        List<WebSocketFrame> frames = readAllFromServer(channel);

        assertThat(frames).as("Should send the text and one Pong").hasSize(2);
        assertThat(frames.get(1).content().toString(StandardCharsets.UTF_8)).as("Pong payload").isEqualTo("ping");
        frames.forEach(WebSocketFrame::release);
    }
}
