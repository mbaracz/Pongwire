package pl.mbaracz.pongwire;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Represents a WebSocket session with a client, maintaining connection details and enabling message sending.
 *
 * @param <T> The type of messages to be sent and received.
 * @param <D> The type of the session context.
 */
public class WebSocketSession<T, D> {

    static final int MIN_CLOSE_STATUS_CODE = 1000;
    static final int MAX_CLOSE_STATUS_CODE = 4999;
    private static final int MAX_CLOSE_REASON_LENGTH = 123;

    private final BiFunction<Supplier<byte[]>, ChannelHandlerContext, ChannelFuture> messageSender;
    private final Function<T, byte[]> encoder;
    private final ChannelHandlerContext channelContext;
    private final D context;
    private final Duration closeTimeout;
    private final Instant connectedSince;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    // Epoch milliseconds, 0 until the first message, so receiving a message allocates nothing
    private volatile long lastMessageMillis;
    private volatile String subprotocol;

    /**
     * Constructs a new WebSocketSession.
     */
    WebSocketSession(
        ChannelHandlerContext channelContext,
        BiFunction<Supplier<byte[]>, ChannelHandlerContext, ChannelFuture> messageSender,
        Function<T, byte[]> encoder,
        D context,
        Duration closeTimeout
    ) {
        this.channelContext = channelContext;
        this.connectedSince = Instant.now();
        this.messageSender = messageSender;
        this.encoder = encoder;
        this.context = context;
        this.closeTimeout = closeTimeout;
    }

    /**
     * @return The instant when the connection was established.
     */
    public Instant getConnectedSince() {
        return connectedSince;
    }

    /**
     * @return The instant when the last message was received, or null if no message has been received.
     */
    public Instant getLastMessageTime() {
        long millis = lastMessageMillis;
        return millis == 0 ? null : Instant.ofEpochMilli(millis);
    }

    /**
     * Updates the last message time to the current date and time.
     */
    void updateLastMessageTime() {
        this.lastMessageMillis = System.currentTimeMillis();
    }

    /**
     * Returns the context of this session, given by the upgrade handler when it accepted the upgrade.
     *
     * @return The session context, or null if the upgrade was accepted without one.
     */
    public D getContext() {
        return context;
    }

    /**
     * Returns the subprotocol selected during the handshake.
     *
     * @return The selected subprotocol, or null if none was selected.
     */
    public String getSubprotocol() {
        return subprotocol;
    }

    /**
     * Sets the subprotocol selected during the handshake.
     */
    void setSubprotocol(String subprotocol) {
        this.subprotocol = subprotocol;
    }

    /**
     * Returns the ChannelHandlerContext associated with this session.
     *
     * @return The ChannelHandlerContext.
     */
    ChannelHandlerContext getChannelContext() {
        return channelContext;
    }

    /**
     * Checks whether the connection is currently writable according to Netty's write buffer watermarks.
     * This is a transient backpressure signal and may change immediately after this method returns.
     *
     * @return True if the connection is currently writable, false otherwise.
     */
    public boolean isWritable() {
        return channelContext.channel().isWritable();
    }

    /**
     * Sends a message to the client associated with this session.
     * The result of the write is not reported, use {@link #sendMessageAsync(Object)}
     * to find out whether it succeeded. The message is encoded later on the event loop, so it must not be changed
     * after this call.
     *
     * @param message The message to be sent.
     * @throws NullPointerException If the message is null.
     */
    public void sendMessage(T message) {
        Objects.requireNonNull(message, "Message must not be null!");
        messageSender.apply(() -> encoder.apply(message), channelContext);
    }

    /**
     * Sends a message the server already encoded, e.g. once for every session of a broadcast.
     *
     * @param payload the encoded message.
     */
    void sendEncoded(byte[] payload) {
        messageSender.apply(() -> payload, channelContext);
    }

    /**
     * Sends a message to the client associated with this session.
     * The message is encoded later on the event loop, so it must not be changed after this call.
     *
     * @param message The message to be sent.
     * @return A stage that completes when the message is written, or exceptionally
     * when encoding, writing, or backpressure rejection fails the send.
     * It usually completes on the event loop, so use the async methods of the stage for blocking continuations.
     * @throws NullPointerException If the message is null.
     */
    public CompletionStage<Void> sendMessageAsync(T message) {
        Objects.requireNonNull(message, "Message must not be null!");
        CompletableFuture<Void> result = new CompletableFuture<>();

        try {
            whenDone(messageSender.apply(() -> encoder.apply(message), channelContext), write -> {
                if (write.isSuccess()) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(write.cause());
                }
            });
        } catch (RuntimeException exception) {
            // Report encoder failures through the stage too, so callers handle every failure in one place
            result.completeExceptionally(exception);
        }

        return result;
    }

    /**
     * Closes the session with a close frame carrying the given status code and reason.
     * The connection is closed once the client answers with its close frame, or after the close timeout.
     *
     * @param code   The status code of the close frame.
     * @param reason The reason of the close frame.
     * @return A stage that completes once the session was removed from the server and the close handler was called,
     * or queued on the callback executor. It usually completes on the event loop, so use the async methods of the
     * stage for blocking continuations.
     * @throws IllegalArgumentException If the code is not a valid close status code,
     *                                  or the reason is longer than 123 bytes in UTF-8.
     */
    public CompletionStage<Void> close(int code, String reason) {
        // Netty accepts every code from 3000 and every negative one, which it writes as an unsigned short,
        // but RFC 6455 defines the codes from 1000 to 4999 (section 7.4)
        if (code < MIN_CLOSE_STATUS_CODE || code > MAX_CLOSE_STATUS_CODE) {
            throw new IllegalArgumentException("Invalid close status code: " + code);
        }

        // A control frame carries at most 125 bytes, two of them the code (RFC 6455, section 5.5)
        if (reason != null && reason.getBytes(StandardCharsets.UTF_8).length > MAX_CLOSE_REASON_LENGTH) {
            throw new IllegalArgumentException("Close reason must not be longer than 123 bytes in UTF-8");
        }

        CloseWebSocketFrame closeFrame = new CloseWebSocketFrame(code, reason);
        ClosingHandshake.start(channelContext.channel(), closeFrame, closeTimeout);

        return closed.copy();
    }

    /**
     * Completes the stages returned by {@link #close(int, String)}, once the server handled the closure of the session.
     */
    void markClosed() {
        closed.complete(null);
    }

    /**
     * Runs the action once the future is done. A completed future is handled right away, because a future of a
     * terminated event loop, e.g. after the server stopped, cannot notify its listeners anymore.
     * The future may complete and the event loop terminate between the check and the registration of the listener,
     * so the check is repeated afterwards and the action runs once, whichever path reaches it first.
     *
     * @param future the future to wait for.
     * @param action the action receiving the done future.
     */
    private static void whenDone(ChannelFuture future, Consumer<ChannelFuture> action) {
        AtomicBoolean done = new AtomicBoolean();
        Runnable once = () -> {
            if (done.compareAndSet(false, true)) {
                action.accept(future);
            }
        };

        if (!future.isDone()) {
            future.addListener(_ -> once.run());
        }

        if (future.isDone()) {
            once.run();
        }
    }
}
