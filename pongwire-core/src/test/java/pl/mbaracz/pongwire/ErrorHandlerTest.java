package pl.mbaracz.pongwire;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import pl.mbaracz.pongwire.configuration.WebSocketServerConfiguration;
import pl.mbaracz.pongwire.handler.ErrorHandler;
import pl.mbaracz.pongwire.message.MessageDecodeException;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class ErrorHandlerTest {

    @Test
    void shouldNotifyErrorHandlerOnceBeforeObserverWhenDecoderFails() {
        MessageDecodeException failure = new MessageDecodeException("decoder failed", new byte[0], null);
        List<FailureCall> calls = new ArrayList<>();
        TestConnection connection = connect(
            false,
            configurer -> configurer.setMessageDecoder(_ -> {
                throw failure;
            }),
            (session, exception) -> calls.add(new FailureCall("handler", session, exception)),
            observerRecording(calls)
        );

        Util.sendFromClient(connection.channel(), new TextWebSocketFrame("message"));

        assertFailureReportedToHandlerAndObserver(calls, connection.session(), failure);
    }

    @Test
    void shouldNotifyErrorHandlerOnceBeforeObserverWhenWriteFails() {
        IllegalStateException failure = new IllegalStateException("write failed");
        List<FailureCall> calls = new ArrayList<>();
        TestConnection connection = connect(
            false,
            _ -> {},
            (session, exception) -> calls.add(new FailureCall("handler", session, exception)),
            observerRecording(calls)
        );
        connection.channel().pipeline().addFirst(new FailingWriteHandler(failure));

        CompletableFuture<Void> send = connection.session().sendMessageAsync("message").toCompletableFuture();

        assertThatThrownBy(send::join)
            .isInstanceOf(CompletionException.class)
            .cause()
            .isSameAs(failure);
        assertFailureReportedToHandlerAndObserver(calls, connection.session(), failure);
    }

    @Test
    void shouldNotReportSendFailureWhenConnectionClosedWithoutCloseFrame() {
        List<FailureCall> calls = new ArrayList<>();
        TestConnection connection = connect(
            false,
            _ -> {},
            (session, exception) -> calls.add(new FailureCall("handler", session, exception)),
            observerRecording(calls)
        );

        // The connection drops without a close frame, then a late message is sent to the closed session
        connection.channel().close();
        CompletableFuture<Void> send = connection.session().sendMessageAsync("message").toCompletableFuture();

        assertThatThrownBy(send::join).as("Send should still fail").isInstanceOf(CompletionException.class);
        assertThat(calls).as("Closed session should not report the failure").isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldIgnoreErrorHandlerFailureAndApplyOriginalClosePolicy(boolean closeOnException) {
        MessageDecodeException failure = new MessageDecodeException("decoder failed", new byte[0], null);
        AtomicInteger handlerCalls = new AtomicInteger();
        List<Throwable> observedFailures = new ArrayList<>();
        TestConnection connection = connect(
            closeOnException,
            configurer -> configurer.setMessageDecoder(_ -> {
                throw failure;
            }),
            (_, _) -> {
                handlerCalls.incrementAndGet();
                throw new AssertionError("error handler failed");
            },
            new WebSocketServerObserver<>() {
                @Override
                public void exception(WebSocketSession<String, Object> session, Throwable exception) {
                    observedFailures.add(exception);
                }
            }
        );

        Util.sendFromClient(connection.channel(), new TextWebSocketFrame("message"));

        assertThat(handlerCalls).hasValue(1);
        assertThat(observedFailures).containsExactly(failure);

        if (closeOnException) {
            // The session is closed with 1007, then the client answers the close frame
            CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(connection.channel()))
                .asInstanceOf(type(CloseWebSocketFrame.class))
                .actual();
            assertThat(closeFrame.statusCode()).isEqualTo(WebSocketCloseStatus.INVALID_PAYLOAD_DATA.code());
            closeFrame.release();
            Util.sendFromClient(connection.channel(), new CloseWebSocketFrame(WebSocketCloseStatus.INVALID_PAYLOAD_DATA));
        }

        assertThat(connection.channel().isOpen()).isEqualTo(!closeOnException);
        assertThat(connection.server().getConnectedSessions().contains(connection.session())).isEqualTo(!closeOnException);
    }

    private static void assertFailureReportedToHandlerAndObserver(
        List<FailureCall> calls,
        WebSocketSession<String, Object> session,
        Throwable failure
    ) {
        assertThat(calls).extracting(FailureCall::recipient).containsExactly("handler", "observer");
        assertThat(calls).allSatisfy(call -> {
            assertThat(call.session()).isSameAs(session);
            assertThat(call.exception()).isSameAs(failure);
        });
    }

    private static WebSocketServerObserver<String, Object> observerRecording(List<FailureCall> calls) {
        return new WebSocketServerObserver<>() {
            @Override
            public void exception(WebSocketSession<String, Object> session, Throwable exception) {
                calls.add(new FailureCall("observer", session, exception));
            }
        };
    }

    private static TestConnection connect(
        boolean closeOnException,
        Consumer<WebSocketServerConfiguration.Builder<String>> configuration,
        ErrorHandler<String, Object> errorHandler,
        WebSocketServerObserver<String, Object> observer
    ) {
        List<WebSocketSession<String, Object>> opened = new ArrayList<>();
        WebSocketServer<String, Object> server = new WebSocketServer<String, Object>()
            .configure(configurer -> {
                configurer
                    .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                    .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                    .setCloseOnException(closeOnException);
                configuration.accept(configurer);
            })
            .onOpen(opened::add)
            .onError(errorHandler)
            .observer(observer);

        EmbeddedChannel channel = Util.connect(server);
        return new TestConnection(server, channel, opened.getFirst());
    }

    private record FailureCall(
        String recipient,
        WebSocketSession<String, Object> session,
        Throwable exception
    ) {
    }

    private record TestConnection(
        WebSocketServer<String, Object> server,
        EmbeddedChannel channel,
        WebSocketSession<String, Object> session
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
