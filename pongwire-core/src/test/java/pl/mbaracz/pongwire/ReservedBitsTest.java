package pl.mbaracz.pongwire;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class ReservedBitsTest {

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

    // RSV1, RSV2 and RSV3, the three reserved bits of a frame
    @ParameterizedTest
    @ValueSource(ints = {0b100, 0b010, 0b001})
    void shouldCloseConnectionWithProtocolErrorWhenFrameHasReservedBitSet(int rsv) {
        EmbeddedChannel channel = Util.connect(server);

        // Send a text frame with a reserved bit set, although no extension was negotiated
        Util.sendFromClient(channel, new TextWebSocketFrame(true, rsv, Unpooled.copiedBuffer("Hello", StandardCharsets.UTF_8)));

        // Assert connection was failed with a protocol error close frame
        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send protocol error status").isEqualTo(WebSocketCloseStatus.PROTOCOL_ERROR.code());
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(closeCodes).containsExactly(closeFrame.statusCode());
        assertThat(received).as("Message should not be delivered").isEmpty();
        closeFrame.release();
    }
}
