package pl.mbaracz.pongwire;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class CloseHandlerTest {

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
            )
            .onClose((_, reason, code) -> closes.add(new Close(code, reason)));
    }

    @Test
    void shouldReceiveSameCodeAndReasonOnServerSideWhenCloseFrameIsSent() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        WebSocketCloseStatus status = WebSocketCloseStatus.NORMAL_CLOSURE;
        String reasonText = "foo";

        server.onClose((_, reason, code) -> {
            assertThat(reason).as("Reason should be the same").isEqualTo(reasonText);
            assertThat(code).as("Status code should be the same").isEqualTo(status.code());
            latch.countDown();
        });

        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Construct close frame and send
        CloseWebSocketFrame closeWebSocketFrame = new CloseWebSocketFrame(status, reasonText);
        channel.writeInbound(closeWebSocketFrame);

        // Read outgoing close frame
        CloseWebSocketFrame outgoingFrame = channel.readOutbound();

        // Assert expected behaviour
        assertThat(outgoingFrame).as("Outbound frame should not be null").isNotNull();
        assertThat(outgoingFrame.reasonText()).as("Reason should be the same").isEqualTo(reasonText);
        assertThat(outgoingFrame.statusCode()).as("Status code should be the same").isEqualTo(status.code());
        assertThat(latch.await(1, TimeUnit.SECONDS)).as("Did not receive expected message from server").isTrue();
        outgoingFrame.release();
    }

    @ParameterizedTest
    @ValueSource(ints = {999, 1005, 2999, 5000, 65535})
    void shouldFailConnectionWithProtocolErrorWhenClientSendsInvalidCloseCode(int code) {
        EmbeddedChannel channel = Util.connect(server);

        Util.sendFromClient(channel, new CloseWebSocketFrame(true, 0, Unpooled.buffer(2).writeShort(code)));

        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel))
            .as("Should answer with a close frame")
            .asInstanceOf(type(CloseWebSocketFrame.class))
            .actual();
        assertThat(closeFrame.statusCode()).as("Close status").isEqualTo(WebSocketCloseStatus.PROTOCOL_ERROR.code());
        closeFrame.release();
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(closes).as("Close handler should receive the protocol error").singleElement()
            .extracting(Close::code).isEqualTo(WebSocketCloseStatus.PROTOCOL_ERROR.code());
    }

    @Test
    void shouldRejectCloseCodeAboveRfcRange() {
        WebSocketSession<String, Object> session = server.getSessionByChannelId(Util.connect(server).id());

        assertThatThrownBy(() -> session.close(5000, "")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectCloseReasonLongerThan123BytesInUtf8() {
        WebSocketSession<String, Object> session = server.getSessionByChannelId(Util.connect(server).id());

        // 62 two-byte characters are 124 bytes, although the string is shorter than 123 characters
        assertThatThrownBy(() -> session.close(1000, "ż".repeat(62))).isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> session.close(1000, "x".repeat(123))).doesNotThrowAnyException();
    }

    @Test
    void shouldCallCloseHandlerOnceWhenCloseFrameIsSent() {
        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Send close frame, the server answers it and closes the connection
        channel.writeInbound(new CloseWebSocketFrame(WebSocketCloseStatus.NORMAL_CLOSURE, "foo"));

        // Assert the close frame was reported once, not again when the connection became inactive
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(closes).as("Close handler should be called once").isEqualTo(List.of(new Close(1000, "foo")));
    }

    @Test
    void shouldReportAbnormalClosureWhenConnectionDropsWithoutCloseFrame() {
        AtomicBoolean registeredOnClose = new AtomicBoolean(true);

        server.onClose((session, reason, code) -> {
            closes.add(new Close(code, reason));
            registeredOnClose.set(server.getConnectedSessions().contains(session) || server.isSubscribed(session, "topic"));
        });

        // Construct channel, perform handshake and subscribe to a topic
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");
        server.subscribe(server.getSessionByChannelId(channel.id()), "topic");

        // Drop the connection without a close frame
        channel.close();

        // Assert abnormal closure was reported once, after the session was cleaned up
        assertThat(closes).as("Close handler should report abnormal closure").isEqualTo(List.of(new Close(1006, "Abnormal closure")));
        assertThat(registeredOnClose.get()).as("Session should be cleaned up before the close handler is called").isFalse();
        assertThat(server.getTopics()).as("Empty topic should be removed").doesNotContain("topic");
    }

    @Test
    void shouldReportNoStatusReceivedWhenCloseFrameHasNoStatusCode() {
        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.connect(server);

        // Send close frame without a status code
        Util.sendFromClient(channel, new CloseWebSocketFrame());

        // Assert 1005 is reported instead of -1
        assertThat(channel.attr(CloseInfo.KEY).get()).isEqualTo(new CloseInfo(1005, ""));
        assertThat(closes).as("Close handler should report no status received").isEqualTo(List.of(new Close(1005, "")));
    }

    @Test
    void shouldNotCallCloseHandlerWhenConnectionClosesBeforeHandshake() {
        // Open and close a connection without a handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        channel.close();

        // Assert no session was reported as closed
        assertThat(closes).as("Close handler should not be called").isEmpty();
    }

    @Test
    void shouldReceiveSameCodeAndReasonWhenCloseFrameIsSentOverEncodedConnection() {
        // Connect through the server's pipeline, where the echoed close frame is encoded
        EmbeddedChannel channel = Util.connect(server);

        // Send encoded close frame
        Util.sendFromClient(channel, new CloseWebSocketFrame(WebSocketCloseStatus.NORMAL_CLOSURE, "foo"));

        // Assert status was read before echoing the frame consumed it
        assertThat(closes).as("Close handler should receive the client's status").isEqualTo(List.of(new Close(1000, "foo")));
    }

    @Test
    void shouldReportGoingAwayToCloseHandlerWhenServerStops() {
        server.listen(0);

        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Stop the server, which closes the session with a going away close frame
        server.stop();

        // Assert the server's close status was reported once instead of an abnormal closure
        assertThat(closes).as("Close handler should receive going away status").isEqualTo(List.of(new Close(1001, "Endpoint unavailable")));
    }
}
