package pl.mbaracz.pongwire;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class CallbackFailureTest {

    // Callbacks run when the test decides, on the test thread, and are rejected once the executor is shut down
    private final Queue<Runnable> callbacks = new ArrayDeque<>();
    private final List<Throwable> errors = new ArrayList<>();
    private final List<Integer> closeCodes = new ArrayList<>();
    private boolean executorShutDown;
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        Executor executor = task -> {
            if (executorShutDown) {
                throw new RejectedExecutionException("Executor is shut down");
            }
            callbacks.add(task);
        };

        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                .setCallbackExecutor(executor)
            )
            .onError((_, exception) -> errors.add(exception))
            .onClose((_, _, code) -> closeCodes.add(code));
    }

    /**
     * Runs the pending callbacks, then the tasks they scheduled on the event loop.
     */
    private void runCallbacks(EmbeddedChannel channel) {
        while (!callbacks.isEmpty()) {
            callbacks.remove().run();
        }

        channel.runPendingTasks();
    }

    @Test
    void shouldReportOpenHandlerFailureWhenCallbacksRunOnEventLoop() {
        IllegalStateException failure = new IllegalStateException("open failed");
        server
            .configure(configurer -> configurer.setCallbackExecutor(null))
            .onOpen(_ -> {
                throw failure;
            });

        Util.connect(server);

        assertThat(errors).as("Error handler should receive the failure").containsExactly(failure);
    }

    @Test
    void shouldReportCloseHandlerFailureWhenItRunsAfterChannelClosed() {
        IllegalStateException failure = new IllegalStateException("cleanup failed");
        server.onClose((_, _, _) -> {
            throw failure;
        });
        EmbeddedChannel channel = Util.connect(server);

        channel.close();
        runCallbacks(channel);

        assertThat(errors).as("Error handler should receive the failure").containsExactly(failure);
    }

    @Test
    void shouldReportErrorThrownByMessageHandler() {
        AssertionError failure = new AssertionError("handler failed");
        server.onMessage((_, _) -> {
            throw failure;
        });
        EmbeddedChannel channel = Util.connect(server);

        Util.sendFromClient(channel, new TextWebSocketFrame("message"));
        runCallbacks(channel);

        assertThat(errors).as("Error handler should receive the error").containsExactly(failure);
    }

    @Test
    void shouldCloseSessionWithTryAgainLaterWhenExecutorRejectsMessage() {
        List<String> received = new ArrayList<>();
        server.onMessage((_, message) -> received.add(message));
        EmbeddedChannel channel = Util.connect(server);
        executorShutDown = true;

        Util.sendFromClient(channel, new TextWebSocketFrame("message"));

        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel))
            .as("Should send a close frame")
            .asInstanceOf(type(CloseWebSocketFrame.class))
            .actual();
        assertThat(closeFrame.statusCode()).as("Close status").isEqualTo(WebSocketCloseStatus.TRY_AGAIN_LATER.code());
        closeFrame.release();
        assertThat(errors).as("Error handler should receive the rejection").singleElement().isInstanceOf(RejectedExecutionException.class);
        assertThat(received).as("Message should not be handled").isEmpty();
    }

    @Test
    void shouldRunCloseHandlerOnEventLoopWhenExecutorRejectsIt() {
        EmbeddedChannel channel = Util.connect(server);
        executorShutDown = true;

        channel.close();

        assertThat(closeCodes).as("Close handler should still run").containsExactly(WebSocketCloseStatus.ABNORMAL_CLOSURE.code());
    }

    @Test
    void shouldCloseSessionOnceAndKeepReadingWhenExecutorRejectsQueuedMessages() {
        List<String> received = new ArrayList<>();
        server
            .configure(configurer -> configurer.setMaxPendingCallbacks(2))
            .onMessage((_, message) -> received.add(message));
        EmbeddedChannel channel = Util.connect(server);
        Util.sendFromClient(channel, new TextWebSocketFrame("first"), new TextWebSocketFrame("second"), new TextWebSocketFrame("third"));
        assertThat(channel.config().isAutoRead()).as("Reading should be paused").isFalse();

        // The executor shuts down while the first message is handled, the queued ones are rejected later
        executorShutDown = true;
        runCallbacks(channel);

        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel))
            .as("Should send a close frame")
            .asInstanceOf(type(CloseWebSocketFrame.class))
            .actual();
        assertThat(closeFrame.statusCode()).as("Close status").isEqualTo(WebSocketCloseStatus.TRY_AGAIN_LATER.code());
        closeFrame.release();
        assertThat(errors).as("Rejection should be reported once").singleElement().isInstanceOf(RejectedExecutionException.class);
        assertThat(channel.config().isAutoRead()).as("Rejected callbacks should not keep reading paused").isTrue();
        assertThat(received).as("Only the first message should be handled").containsExactly("first");
    }

    @Test
    void shouldFailSubscribeStageWhenMessageIsHandledAfterDisconnect() {
        List<CompletableFuture<Void>> subscriptions = new ArrayList<>();
        server.onMessage((session, _) -> subscriptions.add(server.subscribe(session, "topic").toCompletableFuture()));
        EmbeddedChannel channel = Util.connect(server);
        Util.sendFromClient(channel, new TextWebSocketFrame("message"));

        // The client disconnects before the queued onMessage runs
        channel.close();
        runCallbacks(channel);

        assertThat(subscriptions).as("onMessage should run").hasSize(1);
        assertThat(subscriptions.getFirst()).as("Subscribe should fail through its stage").isCompletedExceptionally();
        assertThat(errors).as("Subscribe should not throw from the callback").isEmpty();
    }

    @Test
    void shouldRunOnlyCloseCallbackWhenExecutorRejectsCallbacksOfClosedSession() {
        List<String> received = new ArrayList<>();
        server.onMessage((_, message) -> received.add(message));
        EmbeddedChannel channel = Util.connect(server);
        Util.sendFromClient(channel, new TextWebSocketFrame("first"), new TextWebSocketFrame("second"));

        // The client disconnects and the executor shuts down while the first message is handled
        channel.close();
        executorShutDown = true;
        runCallbacks(channel);

        assertThat(received).as("The message queued behind the rejection should be dropped").containsExactly("first");
        assertThat(closeCodes).as("The close callback should still run").containsExactly(WebSocketCloseStatus.ABNORMAL_CLOSURE.code());
        assertThat(errors).as("Rejection should be reported once").singleElement().isInstanceOf(RejectedExecutionException.class);
    }

    @Test
    @Timeout(10)
    void shouldReportCloseCallbackFailureWhenItIsRejectedAfterEventLoopTerminated() throws Exception {
        IllegalStateException failure = new IllegalStateException("close failed");
        CountDownLatch received = new CountDownLatch(1);
        server
            .onMessage((_, _) -> {})
            .onClose((_, _, _) -> {
                throw failure;
            })
            .observer(new WebSocketServerObserver<>() {
                @Override
                public void messageReceived(WebSocketSession<String, Object> session) {
                    received.countDown();
                }
            });
        server.listen(0);
        HttpClient client = HttpClient.newBuilder().build();

        try {
            WebSocket webSocket = client.newWebSocketBuilder()
                .buildAsync(URI.create("ws://127.0.0.1:" + server.getLocalAddress().getPort()), new WebSocket.Listener() {
                })
                .get(5, TimeUnit.SECONDS);
            webSocket.sendText("message", true).get(5, TimeUnit.SECONDS);

            // The close callback queues behind the message callback, then stop() terminates the event loops
            // and the executor rejects the close callback once the message callback ran
            assertThat(received.await(5, TimeUnit.SECONDS)).as("Message should be received").isTrue();
            server.stop();
            executorShutDown = true;
            callbacks.remove().run();

            assertThat(errors).as("The failure of the close callback should reach the error handler").containsExactly(failure);
        } finally {
            client.shutdownNow();
        }
    }
}
