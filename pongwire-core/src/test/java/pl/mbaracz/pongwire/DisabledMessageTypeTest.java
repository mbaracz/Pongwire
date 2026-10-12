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

class DisabledMessageTypeTest {

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
    void shouldCloseConnectionWithInvalidMessageTypeWhenTextMessagesAreDisabled() {
        server.configure(configurer -> configurer
            .setAllowTextFrames(false)
            .setAllowBinaryFrames(true)
        );

        EmbeddedChannel channel = Util.connect(server);

        Util.sendFromClient(channel, new TextWebSocketFrame("Hello"));

        // Assert connection was closed with an invalid message type close frame
        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send invalid message type status").isEqualTo(WebSocketCloseStatus.INVALID_MESSAGE_TYPE.code());
        closeFrame.release();

        // Client answers the close frame, which completes the closing handshake
        Util.sendFromClient(channel, new CloseWebSocketFrame(WebSocketCloseStatus.INVALID_MESSAGE_TYPE));

        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(received).as("Message should not be delivered").isEmpty();
        assertThat(closeCodes)
            .as("Close handler should get the invalid message type status")
            .isEqualTo(List.of(WebSocketCloseStatus.INVALID_MESSAGE_TYPE.code()));
    }

    @Test
    void shouldCloseConnectionWithInvalidMessageTypeWhenBinaryMessagesAreDisabled() {
        server.configure(configurer -> configurer
            .setAllowTextFrames(true)
            .setAllowBinaryFrames(false)
        );

        EmbeddedChannel channel = Util.connect(server);

        Util.sendFromClient(channel, new BinaryWebSocketFrame(Unpooled.copiedBuffer("Hello", StandardCharsets.UTF_8)));

        // Assert connection was closed with an invalid message type close frame
        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send invalid message type status").isEqualTo(WebSocketCloseStatus.INVALID_MESSAGE_TYPE.code());
        closeFrame.release();

        // Client answers the close frame, which completes the closing handshake
        Util.sendFromClient(channel, new CloseWebSocketFrame(WebSocketCloseStatus.INVALID_MESSAGE_TYPE));

        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(received).as("Message should not be delivered").isEmpty();
        assertThat(closeCodes)
            .as("Close handler should get the invalid message type status")
            .isEqualTo(List.of(WebSocketCloseStatus.INVALID_MESSAGE_TYPE.code()));
    }
}
