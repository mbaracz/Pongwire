package pl.mbaracz.pongwire;

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

class Utf8ValidationTest {

    private final List<String> received = new ArrayList<>();
    private final List<Integer> closeCodes = new ArrayList<>();
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            )
            .onMessage((_, message) -> received.add(message))
            .onClose((_, _, code) -> closeCodes.add(code));
    }

    @Test
    void shouldCloseConnectionWithInvalidPayloadDataWhenTextFrameIsNotValidUtf8() {
        EmbeddedChannel channel = Util.connect(server);

        // Send a text frame ending with 0xFF, a byte that never appears in UTF-8
        Util.sendFromClient(channel, new TextWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{'H', 'i', (byte) 0xFF})));

        // Assert connection was failed with an invalid payload data close frame
        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send invalid payload data status").isEqualTo(WebSocketCloseStatus.INVALID_PAYLOAD_DATA.code());
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(closeCodes).containsExactly(closeFrame.statusCode());
        assertThat(received).as("Message should not be delivered").isEmpty();
        closeFrame.release();
    }

    @Test
    void shouldCloseConnectionWithInvalidPayloadDataWhenInvalidUtf8IsSplitAcrossFragments() {
        EmbeddedChannel channel = Util.connect(server);

        // End the first fragment with the lead byte of a two byte character (0xCE),
        // which the second fragment continues with an ASCII byte instead of a continuation byte
        Util.sendFromClient(channel,
            new TextWebSocketFrame(false, 0, Unpooled.wrappedBuffer(new byte[]{'H', 'i', (byte) 0xCE})),
            new ContinuationWebSocketFrame(true, 0, Unpooled.wrappedBuffer(new byte[]{'A'}))
        );

        // Assert connection was failed with an invalid payload data close frame
        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send invalid payload data status").isEqualTo(WebSocketCloseStatus.INVALID_PAYLOAD_DATA.code());
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(received).as("Message should not be delivered").isEmpty();
        closeFrame.release();
    }

    @Test
    void shouldDeliverWholeMessageWhenValidUtf8IsSplitAcrossFragments() {
        EmbeddedChannel channel = Util.connect(server);

        byte[] message = "κόσμε".getBytes(StandardCharsets.UTF_8);

        // Split the message inside its first character, which takes two bytes
        Util.sendFromClient(channel,
            new TextWebSocketFrame(false, 0, Unpooled.wrappedBuffer(message, 0, 1)),
            new ContinuationWebSocketFrame(true, 0, Unpooled.wrappedBuffer(message, 1, message.length - 1))
        );

        // Assert fragments were delivered as one message
        assertThat(received).as("Should receive one assembled message").isEqualTo(List.of("κόσμε"));
        assertThat(channel.isOpen()).as("Channel should stay open").isTrue();
    }
}
