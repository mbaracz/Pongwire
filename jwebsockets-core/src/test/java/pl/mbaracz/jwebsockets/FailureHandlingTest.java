package pl.mbaracz.jwebsockets;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import pl.mbaracz.jwebsockets.configuration.WebSocketServerConfiguration;
import pl.mbaracz.jwebsockets.handler.MessageHandler;
import pl.mbaracz.jwebsockets.message.MessageDecodeException;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class FailureHandlingTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldHandleDecoderFailureConsistently(boolean closeOnException) {
        MessageDecodeException failure = new MessageDecodeException("decoder failed", new byte[0], null);
        TestConnection connection = connect(closeOnException, configurer -> configurer.setMessageDecoder(_ -> {
            throw failure;
        }));

        Util.sendFromClient(connection.channel(), new TextWebSocketFrame("message"));

        assertFailureWasReportedAndSessionClosedWithStatus(connection, failure, closeOnException, WebSocketCloseStatus.INVALID_PAYLOAD_DATA);
    }

    @Test
    void shouldReportDecoderFailureWithPayloadOfMessage() {
        IllegalArgumentException failure = new IllegalArgumentException("decoder failed");
        TestConnection connection = connect(false, configurer -> configurer.setMessageDecoder(_ -> {
            throw failure;
        }));

        Util.sendFromClient(connection.channel(), new TextWebSocketFrame("message"));

        MessageDecodeException reported = assertThat(connection.observedFailures())
            .singleElement()
            .asInstanceOf(type(MessageDecodeException.class))
            .actual();
        assertThat(reported).as("Should keep the decoder failure").hasCause(failure);
        assertThat(reported.getPayload()).as("Should carry the payload").isEqualTo("message".getBytes(StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldHandleEncoderFailureConsistently(boolean closeOnException) {
        IllegalStateException failure = new IllegalStateException("encoder failed");
        TestConnection connection = connect(closeOnException, configurer -> configurer.setMessageEncoder(_ -> {
            throw failure;
        }));

        CompletableFuture<Void> send = connection.session().sendMessageAsync("message").toCompletableFuture();

        assertSendFailedWith(send, failure);
        assertFailureWasReportedAndSessionClosedWithStatus(connection, failure, closeOnException, WebSocketCloseStatus.INTERNAL_SERVER_ERROR);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldReportEncoderFailureWithoutThrowingFromSendMessage(boolean closeOnException) {
        IllegalStateException failure = new IllegalStateException("encoder failed");
        TestConnection connection = connect(closeOnException, configurer -> configurer.setMessageEncoder(_ -> {
            throw failure;
        }));

        assertThatCode(() -> connection.session().sendMessage("message"))
            .doesNotThrowAnyException();

        assertFailureWasReportedAndSessionClosedWithStatus(connection, failure, closeOnException, WebSocketCloseStatus.INTERNAL_SERVER_ERROR);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldHandleMessageHandlerFailureConsistently(boolean closeOnException) {
        IllegalStateException failure = new IllegalStateException("handler failed");
        TestConnection connection = connect(closeOnException, _ -> {}, (_, _) -> {
            throw failure;
        });

        Util.sendFromClient(connection.channel(), new TextWebSocketFrame("message"));

        assertFailureWasReportedAndSessionClosedWithStatus(connection, failure, closeOnException, WebSocketCloseStatus.INTERNAL_SERVER_ERROR);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldHandleWriteFailureConsistently(boolean closeOnException) {
        IllegalStateException failure = new IllegalStateException("write failed");
        TestConnection connection = connect(closeOnException, _ -> {});
        connection.channel().pipeline().addFirst(new FailingWriteHandler(failure));

        CompletableFuture<Void> send = connection.session().sendMessageAsync("message").toCompletableFuture();

        assertSendFailedWith(send, failure);
        assertFailureWasReportedAndConnectionStateMatchesConfiguration(connection, failure, closeOnException);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldCloseWithoutCloseFrameWhenClientConnectionFails(boolean closeOnException) {
        IOException failure = new IOException("Connection reset by peer");
        TestConnection connection = connect(closeOnException, _ -> {});

        connection.channel().pipeline().fireExceptionCaught(failure);

        // A broken connection cannot carry a close frame
        assertThat(Util.readFromServer(connection.channel())).as("Should not send a close frame").isNull();
        assertFailureWasReportedAndConnectionStateMatchesConfiguration(connection, failure, closeOnException);
    }

    @Test
    void shouldReportMessageHandlerFailureAsServerFailureWhateverItsType() {
        // A Netty exception type thrown by the application is still a failure of the server
        DecoderException failure = new DecoderException("handler failed");
        TestConnection connection = connect(true, _ -> {}, (_, _) -> {
            throw failure;
        });

        Util.sendFromClient(connection.channel(), new TextWebSocketFrame("message"));

        assertFailureWasReportedAndSessionClosedWithStatus(connection, failure, true, WebSocketCloseStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void shouldWaitForCloseFrameWhenProtocolIsViolated() {
        TestConnection connection = connect(true, _ -> {});
        EmbeddedChannel channel = connection.channel();
        channel.freezeTime();

        // The close frame for the violation is still waiting behind a client that does not read
        channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void flush(ChannelHandlerContext context) {
            }
        });
        channel.pipeline().fireExceptionCaught(new CorruptedWebSocketFrameException(WebSocketCloseStatus.PROTOCOL_ERROR, "unmasked"));

        assertThat(channel.isOpen()).as("Should not drop the pending close frame").isTrue();

        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();

        assertThat(channel.isOpen()).as("Should close after the close timeout").isFalse();
    }

    private static void assertSendFailedWith(CompletableFuture<Void> send, Throwable failure) {
        assertThatThrownBy(send::join)
            .isInstanceOf(CompletionException.class)
            .cause()
            .isSameAs(failure);
    }

    private static void assertFailureWasReportedAndConnectionStateMatchesConfiguration(
        TestConnection connection,
        Throwable failure,
        boolean closeOnException
    ) {
        assertThat(connection.observedFailures()).containsExactly(failure);
        assertThat(connection.channel().isOpen()).isEqualTo(!closeOnException);
        assertThat(connection.server().getConnectedSessions().contains(connection.session())).isEqualTo(!closeOnException);
    }

    /**
     * Asserts the failure was reported and, with closeOnException, the session is closed with a close frame.
     */
    private static void assertFailureWasReportedAndSessionClosedWithStatus(
        TestConnection connection,
        Throwable failure,
        boolean closeOnException,
        WebSocketCloseStatus status
    ) {
        assertThat(connection.observedFailures()).containsExactly(failure);

        if (closeOnException) {
            CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(connection.channel()))
                .as("Should send a close frame")
                .asInstanceOf(type(CloseWebSocketFrame.class))
                .actual();
            assertThat(closeFrame.statusCode()).as("Close status").isEqualTo(status.code());
            closeFrame.release();

            // The client answers the close frame
            Util.sendFromClient(connection.channel(), new CloseWebSocketFrame(status));
        }

        assertThat(connection.channel().isOpen()).isEqualTo(!closeOnException);
        assertThat(connection.server().getConnectedSessions().contains(connection.session())).isEqualTo(!closeOnException);
    }

    private static TestConnection connect(
        boolean closeOnException,
        Consumer<WebSocketServerConfiguration.Builder<String>> configuration
    ) {
        return connect(closeOnException, configuration, (_, _) -> {});
    }

    private static TestConnection connect(
        boolean closeOnException,
        Consumer<WebSocketServerConfiguration.Builder<String>> configuration,
        MessageHandler<String, Object> messageHandler
    ) {
        List<WebSocketSession<String, Object>> opened = new ArrayList<>();
        List<Throwable> observedFailures = new ArrayList<>();
        WebSocketServer<String, Object> server = new WebSocketServer<String, Object>()
            .configure(configurer -> {
                configurer
                    .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                    .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                    .setCloseOnException(closeOnException);
                configuration.accept(configurer);
            })
            .onOpen(opened::add)
            .onMessage(messageHandler)
            .observer(new WebSocketServerObserver<>() {
                @Override
                public void exception(WebSocketSession<String, Object> session, Throwable exception) {
                    observedFailures.add(exception);
                }
            });

        EmbeddedChannel channel = Util.connect(server);
        return new TestConnection(server, channel, opened.getFirst(), observedFailures);
    }

    private record TestConnection(
        WebSocketServer<String, Object> server,
        EmbeddedChannel channel,
        WebSocketSession<String, Object> session,
        List<Throwable> observedFailures
    ) {
    }

    private static final class FailingWriteHandler extends ChannelOutboundHandlerAdapter {

        private final Throwable failure;

        private FailingWriteHandler(Throwable failure) {
            this.failure = failure;
        }

        @Override
        public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
            ReferenceCountUtil.release(message);
            promise.setFailure(failure);
        }
    }
}
