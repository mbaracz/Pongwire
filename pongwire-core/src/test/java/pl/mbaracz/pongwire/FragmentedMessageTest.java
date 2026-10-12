package pl.mbaracz.pongwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class FragmentedMessageTest {

    private final List<String> received = new ArrayList<>();
    private final List<Integer> closeCodes = new ArrayList<>();
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                .setAllowBinaryFrames(true)
                .setMaxFrameSize(1024)
                .setMaxMessageSize(1024)
            )
            .onMessage((_, message) -> received.add(message))
            .onClose((_, _, code) -> closeCodes.add(code));
    }

    private static ByteBuf utf8(String text) {
        return Unpooled.copiedBuffer(text, StandardCharsets.UTF_8);
    }

    @Test
    void shouldDeliverWholeMessageWhenTextMessageIsFragmented() {
        EmbeddedChannel channel = Util.connect(server);

        // Send text message split into three fragments
        Util.sendFromClient(channel,
            new TextWebSocketFrame(false, 0, utf8("Hello, ")),
            new ContinuationWebSocketFrame(false, 0, utf8("fragmented ")),
            new ContinuationWebSocketFrame(true, 0, utf8("world"))
        );

        // Assert fragments were delivered as one message
        assertThat(received).as("Should receive one assembled message").isEqualTo(List.of("Hello, fragmented world"));
    }

    @Test
    void shouldDeliverWholeMessageWhenBinaryMessageIsFragmented() {
        EmbeddedChannel channel = Util.connect(server);

        // Send binary message split into two fragments
        Util.sendFromClient(channel,
            new BinaryWebSocketFrame(false, 0, utf8("binary ")),
            new ContinuationWebSocketFrame(true, 0, utf8("payload"))
        );

        // Assert fragments were delivered as one message
        assertThat(received).as("Should receive one assembled message").isEqualTo(List.of("binary payload"));
    }

    @Test
    void shouldCloseConnectionWhenFragmentedMessageExceedsMaximumSize() {
        server.configure(configurer -> configurer
            .setMaxFrameSize(16)
            .setMaxMessageSize(16)
        );

        EmbeddedChannel channel = Util.connect(server);

        // Send two fragments of 10 bytes, together exceeding the 16 byte limit
        Util.sendFromClient(channel,
            new TextWebSocketFrame(false, 0, utf8("0123456789")),
            new ContinuationWebSocketFrame(true, 0, utf8("0123456789"))
        );

        // Assert message was rejected with a message too big close frame
        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send message too big status").isEqualTo(WebSocketCloseStatus.MESSAGE_TOO_BIG.code());
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(closeCodes).containsExactly(closeFrame.statusCode());
        assertThat(received).as("Message should not be delivered").isEmpty();
        closeFrame.release();
    }

    @Test
    void shouldCloseConnectionWhenSingleFrameExceedsMaximumSize() {
        server.configure(configurer -> configurer
            .setMaxFrameSize(16)
            .setMaxMessageSize(32)
        );

        EmbeddedChannel channel = Util.connect(server);

        // Send a single 20 byte frame, exceeding the 16 byte limit
        Util.sendFromClient(channel, new TextWebSocketFrame(utf8("01234567890123456789")));

        // Assert message was rejected with a message too big close frame
        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send message too big status").isEqualTo(WebSocketCloseStatus.MESSAGE_TOO_BIG.code());
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(received).as("Message should not be delivered").isEmpty();
        closeFrame.release();
    }

    @Test
    void shouldDeliverFragmentedMessageWithinFrameAndMessageLimits() {
        server.configure(configurer -> configurer
            .setMaxFrameSize(10)
            .setMaxMessageSize(20)
        );

        EmbeddedChannel channel = Util.connect(server);

        Util.sendFromClient(channel,
            new TextWebSocketFrame(false, 0, utf8("0123456789")),
            new ContinuationWebSocketFrame(true, 0, utf8("0123456789"))
        );

        assertThat(received).containsExactly("01234567890123456789");
        assertThat(channel.isOpen()).as("Channel should stay open").isTrue();
    }
}
