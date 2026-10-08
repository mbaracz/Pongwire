package pl.mbaracz.jwebsockets;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class InboundBackpressureTest {

    // Callbacks run when the test decides, on the test thread, like a callback executor that fell behind
    private final Queue<Runnable> callbacks = new ArrayDeque<>();
    private final List<String> received = new ArrayList<>();
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                .setCallbackExecutor(callbacks::add)
                .setMaxPendingCallbacks(4)
            )
            .onMessage((_, message) -> received.add(message));
    }

    private static void send(EmbeddedChannel channel, int count) {
        for (int i = 0; i < count; i++) {
            Util.sendFromClient(channel, new TextWebSocketFrame("message " + i));
        }
    }

    /**
     * Runs the given number of pending callbacks, then the tasks they scheduled on the event loop.
     */
    private void runCallbacks(EmbeddedChannel channel, int count) {
        for (int i = 0; i < count; i++) {
            callbacks.remove().run();
        }

        channel.runPendingTasks();
    }

    @Test
    void shouldKeepReadingWhenPendingCallbacksAreBelowMaximum() {
        EmbeddedChannel channel = Util.connect(server);

        send(channel, 3);

        assertThat(channel.config().isAutoRead()).as("Should keep reading").isTrue();
    }

    @Test
    void shouldStopReadingWhenPendingCallbacksReachMaximum() {
        EmbeddedChannel channel = Util.connect(server);

        send(channel, 4);

        assertThat(channel.config().isAutoRead()).as("Should stop reading").isFalse();
    }

    @Test
    void shouldResumeReadingWhenHalfOfPendingCallbacksHaveRun() {
        EmbeddedChannel channel = Util.connect(server);
        send(channel, 4);

        runCallbacks(channel, 1);
        assertThat(channel.config().isAutoRead()).as("Should not resume above half of the maximum").isFalse();

        runCallbacks(channel, 1);
        assertThat(channel.config().isAutoRead()).as("Should resume at half of the maximum").isTrue();
        assertThat(received).as("Callbacks should run in order").containsExactly("message 0", "message 1");
    }

    @Test
    void shouldStopReadingAgainWhenPendingCallbacksReachMaximumAfterResuming() {
        EmbeddedChannel channel = Util.connect(server);
        send(channel, 4);
        runCallbacks(channel, 2);

        send(channel, 2);

        assertThat(channel.config().isAutoRead()).as("Should stop reading again").isFalse();
    }

    @Test
    void shouldNeverStopReadingWhenCallbacksRunOnEventLoop() {
        server.configure(configurer -> configurer.setCallbackExecutor(null));
        EmbeddedChannel channel = Util.connect(server);

        send(channel, 10);

        assertThat(channel.config().isAutoRead()).as("Should keep reading").isTrue();
        assertThat(received).as("Callbacks should run right away").hasSize(10);
    }

    @Test
    void shouldNotCountRejectedCallbacks() {
        server.configure(configurer -> configurer
            .setCallbackExecutor(_ -> {
                throw new RejectedExecutionException("Executor is shut down");
            })
            .setMaxPendingCallbacks(1)
        );
        EmbeddedChannel channel = Util.connect(server);

        send(channel, 1);

        assertThat(channel.config().isAutoRead()).as("Should keep reading").isTrue();
    }

    @Test
    void shouldHoldBackReadsRequestedByHandlersWhenPendingCallbacksReachMaximum() {
        EmbeddedChannel channel = Util.connect(server);
        send(channel, 4);
        AtomicInteger reads = countReads(channel);

        // The frame aggregator requests a read itself while a fragmented message is incomplete
        channel.pipeline().context(WebSocketFrameAggregator.class).read();

        assertThat(reads).as("Read should be held back").hasValue(0);

        runCallbacks(channel, 2);

        assertThat(reads.get()).as("Resuming should read again").isPositive();
    }

    /**
     * Counts the read requests that reach the transport.
     */
    private static AtomicInteger countReads(EmbeddedChannel channel) {
        AtomicInteger reads = new AtomicInteger();
        channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void read(ChannelHandlerContext context) {
                reads.incrementAndGet();
                context.read();
            }
        });
        return reads;
    }
}
