package pl.mbaracz.pongwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class MessageTest {

    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            );
    }

    @AfterEach
    void tearDown() {
        if (server.isRunning()) {
            server.stop();
        }
    }

    @Test
    void shouldPassMessageToMessageHandlerWhenItIsSentFromClient() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        server.onMessage((_, message) -> {
            assertThat(message).isEqualTo("hello");
            latch.countDown();
        });

        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Construct text frame and send
        String messageToSend = "hello";
        TextWebSocketFrame textFrame = new TextWebSocketFrame(messageToSend);
        channel.writeInbound(textFrame);

        // Await to execute message handler by server
        assertThat(latch.await(1, TimeUnit.SECONDS)).as("Did not receive expected message from server").isTrue();
    }

    @Test
    void shouldSendTextFrameWhenMessageIsSentFromServer() {
        server.listen(8082);

        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Send message to all connected clients
        server.broadcast("hello");

        // Read outgoing frame
        TextWebSocketFrame outbound = channel.readOutbound();

        // Assert received frame content is equal to sent
        assertThat(outbound.text()).isEqualTo("hello");
        outbound.release();
    }

    @Test
    void shouldCloseWithInvalidMessageTypeWhenUserSendsBinaryFrameAndOptionIsNotEnabled() {
        server
            .configure(configurer -> configurer.setCloseOnException(true))
            .onMessage(WebSocketSession::sendMessage)
            .listen(8086);

        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Construct binary frame and send
        ByteBuf byteBuf = Unpooled.wrappedBuffer("hello".getBytes(StandardCharsets.UTF_8));
        BinaryWebSocketFrame frame = new BinaryWebSocketFrame(byteBuf);
        channel.writeInbound(frame);

        // Assert connection was closed with an invalid message type close frame
        CloseWebSocketFrame closeFrame = assertThat(channel.<Object>readOutbound()).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send invalid message type status").isEqualTo(WebSocketCloseStatus.INVALID_MESSAGE_TYPE.code());
        closeFrame.release();

        // Client answers the close frame, which completes the closing handshake
        channel.writeInbound(new CloseWebSocketFrame(WebSocketCloseStatus.INVALID_MESSAGE_TYPE));

        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
    }

    @Test
    void shouldHandleFrameWhenUserSendsBinaryFrameAndOptionIsEnabled() {
        server
            .configure(configurer -> configurer
                .setAllowBinaryFrames(true)
                .setRespondWithBinaryFrame(true)
            )
            .onMessage(WebSocketSession::sendMessage)
            .listen(8080);

        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Construct binary frame and send
        String message = "hello";
        ByteBuf byteBuf = Unpooled.wrappedBuffer(message.getBytes(StandardCharsets.UTF_8));
        BinaryWebSocketFrame frame = new BinaryWebSocketFrame(byteBuf);
        channel.writeInbound(frame);

        // Read outgoing message
        BinaryWebSocketFrame binaryWebSocketFrame = channel.readOutbound();
        String outputMessage = binaryWebSocketFrame.content().toString(StandardCharsets.UTF_8);

        // Assert outgoing message is equal to sent
        assertThat(outputMessage).isEqualTo(message);
        binaryWebSocketFrame.release();
    }

    @Test
    void shouldRespondWithBinaryFrameWhenUserSendsTextFrameAndOptionIsEnabled() {
        server
            .configure(configurer -> configurer
                .setAllowBinaryFrames(true)
                .setRespondWithBinaryFrame(true)
            )
            .onMessage(WebSocketSession::sendMessage)
            .listen(8081);

        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Construct text frame and send
        String message = "hello";
        TextWebSocketFrame textFrame = new TextWebSocketFrame(message);
        channel.writeInbound(textFrame);

        // Read outgoing message
        BinaryWebSocketFrame binaryWebSocketFrame = channel.readOutbound();
        String outputMessage = binaryWebSocketFrame.content().toString(StandardCharsets.UTF_8);

        // Assert outgoing message is equal to sent
        assertThat(outputMessage).isEqualTo(message);
        binaryWebSocketFrame.release();
    }
}
