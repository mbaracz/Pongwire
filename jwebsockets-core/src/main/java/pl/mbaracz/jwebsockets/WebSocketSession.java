package pl.mbaracz.jwebsockets;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Represents a WebSocket session with a client, maintaining connection details and enabling message sending.
 *
 * @param <T> The type of messages to be sent and received.
 * @param <D> The type of the session context.
 */
public class WebSocketSession<T, D> {

    private final BiFunction<T, ChannelHandlerContext, ChannelFuture> messageSender;
    private final ChannelHandlerContext channelContext;
    private final D context;
    private final Duration closeTimeout;
    private final Instant connectedSince;
    private volatile Instant lastMessageTime;
    private volatile String subprotocol;

    /**
     * Constructs a new WebSocketSession.
     */
    WebSocketSession(
        ChannelHandlerContext channelContext,
        BiFunction<T, ChannelHandlerContext, ChannelFuture> messageSender,
        D context,
        Duration closeTimeout
    ) {
        this.channelContext = channelContext;
        this.connectedSince = Instant.now();
        this.messageSender = messageSender;
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
        return lastMessageTime;
    }

    /**
     * Updates the last message time to the current date and time.
     */
    void updateLastMessageTime() {
        this.lastMessageTime = Instant.now();
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
     * to find out whether it succeeded.
     *
     * @param message The message to be sent.
     */
    public void sendMessage(T message) {
        messageSender.apply(message, channelContext);
    }

    /**
     * Sends a message to the client associated with this session.
     *
     * @param message The message to be sent.
     * @return A stage that completes when the message is written, or exceptionally
     * when encoding, writing, or backpressure rejection fails the send.
     */
    public CompletionStage<Void> sendMessageAsync(T message) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        try {
            whenDone(messageSender.apply(message, channelContext), write -> {
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
     * @return A stage that completes when the connection is closed.
     * @throws IllegalArgumentException If the code is not a valid close status code.
     */
    public CompletionStage<Void> close(int code, String reason) {
        CloseWebSocketFrame closeFrame = new CloseWebSocketFrame(code, reason);
        CompletableFuture<Void> result = new CompletableFuture<>();

        whenDone(ClosingHandshake.start(channelContext.channel(), closeFrame, closeTimeout), _ -> result.complete(null));

        return result;
    }

    /**
     * Runs the action once the future is done. A completed future is handled right away, because a future of a
     * terminated event loop, e.g. after the server stopped, cannot notify its listeners anymore.
     *
     * @param future the future to wait for.
     * @param action the action receiving the done future.
     */
    private static void whenDone(ChannelFuture future, Consumer<ChannelFuture> action) {
        if (future.isDone()) {
            action.accept(future);
        } else {
            future.addListener(_ -> action.accept(future));
        }
    }
}
