package pl.mbaracz.pongwire;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.configuration.BackpressurePolicy;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ReplyFlushTest {

    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                .setCallbackExecutor(null)
            )
            .onMessage(WebSocketSession::sendMessage);
    }

    @Test
    void shouldFlushRepliesOnceWhenMessagesArriveInOneRead() {
        EmbeddedChannel channel = Util.connect(server);
        AtomicInteger flushes = new AtomicInteger();
        channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void flush(ChannelHandlerContext context) {
                // A flush with nothing written does not reach the socket
                if (context.channel().unsafe().outboundBuffer().totalPendingWriteBytes() > 0) {
                    flushes.incrementAndGet();
                }
                context.flush();
            }
        });

        Util.sendFromClient(channel, new TextWebSocketFrame("first"), new TextWebSocketFrame("second"), new TextWebSocketFrame("third"));

        assertThat(flushes).as("Should flush once per read").hasValue(1);
        assertThat(countReplies(channel)).as("Should answer every message").isEqualTo(3);
    }

    @Test
    void shouldFlushReplyRightAwayWhenItIsSentOutsideRead() {
        EmbeddedChannel channel = Util.connect(server);

        server.getSessionByChannelId(channel.id()).sendMessage("hello");

        assertThat(countReplies(channel)).as("Reply should be flushed").isEqualTo(1);
    }

    @Test
    void shouldDrainReplyThatFilledBufferBeforeNextReplyOfSameRead() {
        server.configure(configurer -> configurer
            .setWriteBufferWaterMark(1024, 2048)
            .setBackpressurePolicy(BackpressurePolicy.REJECT_NEW)
        );
        EmbeddedChannel channel = Util.connect(server);

        // The first reply alone exceeds the high watermark, the client reads fine
        Util.sendFromClient(channel, new TextWebSocketFrame("x".repeat(4096)), new TextWebSocketFrame("second"));

        // The large reply is encoded into a header and a payload buffer, the rejected reply would leave nothing
        assertThat(countReplies(channel)).as("Should answer both messages").isEqualTo(3);
    }

    /**
     * Counts the frames written by the server.
     * A small frame is encoded into one buffer.
     */
    private static int countReplies(EmbeddedChannel channel) {
        int replies = channel.outboundMessages().size();
        channel.releaseOutbound();
        return replies;
    }
}
