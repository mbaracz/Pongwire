package pl.mbaracz.jwebsockets;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class SessionCloseTest {

    private record Close(int code, String reason) {
    }

    private final List<WebSocketSession<String, Object>> opened = new ArrayList<>();
    private final List<String> received = new ArrayList<>();
    private final List<Close> closes = new ArrayList<>();
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                .setCloseTimeout(Duration.ofSeconds(5))
            )
            .onOpen(opened::add)
            .onMessage((_, message) -> received.add(message))
            .onClose((_, reason, code) -> closes.add(new Close(code, reason)));
    }

    @Test
    void shouldSendCloseFrameAndWaitForClientWhenSessionIsClosed() {
        EmbeddedChannel channel = Util.connect(server);

        CompletableFuture<Void> result = opened.getFirst().close(1000, "done").toCompletableFuture();

        // Assert close frame was sent and the connection waits for the client's close frame
        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send the status code").isEqualTo(1000);
        assertThat(closeFrame.reasonText()).as("Should send the reason").isEqualTo("done");
        assertThat(channel.isOpen()).as("Channel should stay open until the client answers").isTrue();
        assertThat(result.isDone()).as("Stage should not be completed yet").isFalse();
        assertThat(closes).as("Close handler should not be called yet").isEmpty();
        closeFrame.release();
    }

    @Test
    void shouldCloseConnectionWithoutEchoWhenClientAnswersCloseFrame() {
        EmbeddedChannel channel = Util.connect(server);
        CompletableFuture<Void> result = opened.getFirst().close(1000, "done").toCompletableFuture();
        WebSocketFrame closeFrame = Util.readFromServer(channel);
        closeFrame.release();

        // Client answers the close frame of the server
        Util.sendFromClient(channel, new CloseWebSocketFrame(WebSocketCloseStatus.NORMAL_CLOSURE, "bye"));

        // Assert handshake completed: no second close frame, connection closed, server's status reported
        assertThat(Util.readFromServer(channel)).as("Client's close frame should not be echoed").isNull();
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(result.isDone()).as("Stage should be completed").isTrue();
        assertThat(result.isCompletedExceptionally()).as("Stage should complete normally").isFalse();
        assertThat(closes).as("Close handler should be called once with the server's status").isEqualTo(List.of(new Close(1000, "done")));
    }

    @Test
    void shouldCloseConnectionWhenClientDoesNotAnswerWithinCloseTimeout() {
        EmbeddedChannel channel = Util.connect(server);
        channel.freezeTime();

        opened.getFirst().close(1000, "done");

        // Just before the timeout
        channel.advanceTimeBy(4999, TimeUnit.MILLISECONDS);
        channel.runScheduledPendingTasks();
        assertThat(channel.isOpen()).as("Channel should stay open before the timeout").isTrue();

        // Timeout elapsed
        channel.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        channel.runScheduledPendingTasks();
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(closes).as("Close handler should be called once with the server's status").isEqualTo(List.of(new Close(1000, "done")));
    }

    @Test
    void shouldCloseConnectionWhenCloseEchoIsNotWrittenWithinCloseTimeout() {
        EmbeddedChannel channel = Util.connect(server);
        channel.freezeTime();

        // A client that stops reading never receives the echo of its close frame
        channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void flush(ChannelHandlerContext context) {
            }
        });
        Util.sendFromClient(channel, new CloseWebSocketFrame(WebSocketCloseStatus.NORMAL_CLOSURE, "bye"));

        channel.advanceTimeBy(4999, TimeUnit.MILLISECONDS);
        channel.runScheduledPendingTasks();
        assertThat(channel.isOpen()).as("Channel should stay open before the timeout").isTrue();

        channel.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        channel.runScheduledPendingTasks();
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(closes).as("Close handler should be called once with the client's status").isEqualTo(List.of(new Close(1000, "bye")));
    }

    @Test
    void shouldNotSendSecondCloseFrameWhenSessionIsClosedAgain() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = opened.getFirst();

        session.close(1000, "done");
        WebSocketFrame closeFrame = Util.readFromServer(channel);
        closeFrame.release();
        CompletableFuture<Void> result = session.close(1001, "again").toCompletableFuture();

        // Assert second close sent nothing and completes with the first one
        assertThat(Util.readFromServer(channel)).as("Second close frame should not be sent").isNull();
        assertThat(result.isDone()).as("Stage should not be completed yet").isFalse();

        Util.sendFromClient(channel, new CloseWebSocketFrame(WebSocketCloseStatus.NORMAL_CLOSURE));

        assertThat(result.isDone()).as("Stage should be completed").isTrue();
        assertThat(closes).as("Close handler should be called only once").isEqualTo(List.of(new Close(1000, "done")));
    }

    @Test
    void shouldCompleteCloseWhenSessionIsAlreadyClosed() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = opened.getFirst();
        channel.close();

        CompletableFuture<Void> result = session.close(1000, "done").toCompletableFuture();

        // Assert close of a closed connection completed without another callback
        assertThat(result.isDone()).as("Stage should be completed").isTrue();
        assertThat(result.isCompletedExceptionally()).as("Stage should complete normally").isFalse();
        assertThat(closes).as("Close handler should be called only once").hasSize(1);
    }

    @Test
    void shouldDiscardMessagesReceivedAfterCloseFrameWasSent() {
        EmbeddedChannel channel = Util.connect(server);

        opened.getFirst().close(1000, "done");
        Util.sendFromClient(channel, new TextWebSocketFrame("late"));

        assertThat(received).as("Message sent after the close frame should not be delivered").isEmpty();
        assertThat(channel.isOpen()).as("Channel should stay open until the client answers").isTrue();
    }

    @Test
    void shouldNotSendMessagesAfterClosingHandshakeWasStarted() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = opened.getFirst();

        session.close(1000, "done");
        WebSocketFrame closeFrame = Util.readFromServer(channel);
        closeFrame.release();

        CompletableFuture<Void> result = session.sendMessageAsync("late").toCompletableFuture();
        session.sendMessage("late");

        // Assert the close frame stayed the last frame sent
        assertThat(Util.readFromServer(channel)).as("Message should not be sent after the close frame").isNull();
        assertThat(result).as("Send should fail while the session is closing").isCompletedExceptionally();
    }

    @Test
    void shouldRejectMessagesAfterClientInitiatedCloseWhileChannelIsOpen() {
        AtomicReference<CompletableFuture<Void>> send = new AtomicReference<>();
        server.onClose((session, _, _) -> {
            send.set(session.sendMessageAsync("late").toCompletableFuture());
            session.sendMessage("also late");
        });

        AtomicReference<ChannelPromise> closeWrite = new AtomicReference<>();
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");
        channel.pipeline().addLast(holdCloseWrite(closeWrite));

        channel.writeInbound(new CloseWebSocketFrame(WebSocketCloseStatus.NORMAL_CLOSURE, "bye"));

        assertThat(channel.isOpen()).as("Close reply is still pending").isTrue();
        assertThatThrownBy(() -> send.get().join())
            .cause().isInstanceOf(IllegalStateException.class)
            .hasMessage("WebSocket session is closing");
        CloseWebSocketFrame reply = (CloseWebSocketFrame) channel.readOutbound();
        assertThat(reply.statusCode()).isEqualTo(1000);
        reply.release();
        assertThat((Object) channel.readOutbound()).as("No data frame follows Close").isNull();

        closeWrite.get().setSuccess();
    }

    @Test
    void shouldNotSendSecondCloseAfterClientInitiatedCloseWhileChannelIsOpen() {
        server.onClose((session, _, _) -> session.close(1000, "again"));

        AtomicReference<ChannelPromise> closeWrite = new AtomicReference<>();
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");
        channel.pipeline().addLast(holdCloseWrite(closeWrite));

        channel.writeInbound(new CloseWebSocketFrame(WebSocketCloseStatus.NORMAL_CLOSURE, "bye"));

        assertThat(channel.isOpen()).as("Close reply is still pending").isTrue();
        CloseWebSocketFrame reply = (CloseWebSocketFrame) channel.readOutbound();
        assertThat(reply.statusCode()).isEqualTo(1000);
        reply.release();
        assertThat((Object) channel.readOutbound()).as("Only one Close reply is sent").isNull();

        closeWrite.get().setSuccess();
    }

    private static ChannelOutboundHandlerAdapter holdCloseWrite(AtomicReference<ChannelPromise> closeWrite) {
        return new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
                if (message instanceof CloseWebSocketFrame && closeWrite.compareAndSet(null, promise)) {
                    context.write(message, context.newPromise());
                } else {
                    context.write(message, promise);
                }
            }
        };
    }

    @Test
    void shouldThrowWhenCloseCodeIsInvalid() {
        EmbeddedChannel channel = Util.connect(server);

        // 1005 is reserved and must not be sent in a close frame
        assertThatThrownBy(() -> opened.getFirst().close(1005, "invalid")).isInstanceOf(IllegalArgumentException.class);
        assertThat(channel.isOpen()).as("Channel should stay open").isTrue();
    }
}
