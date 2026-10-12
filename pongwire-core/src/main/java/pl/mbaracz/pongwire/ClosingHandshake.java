package pl.mbaracz.pongwire;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.util.AttributeKey;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Closing handshake started by the server (RFC 6455, section 7.1.2). The server sends its close frame, waits for
 * the close frame of the client and only then closes the connection, or closes it when the client does not answer in time.
 */
final class ClosingHandshake {

    private enum State {
        CLIENT_STARTED,
        SERVER_STARTED,
        SERVER_FAILED
    }

    private static final AttributeKey<State> STATE = AttributeKey.valueOf(ClosingHandshake.class, "state");

    private ClosingHandshake() {
    }

    /**
     * Sends the close frame and closes the connection if the client does not answer it within the timeout.
     * Does nothing but release the frame if either peer already started closing the connection.
     *
     * @param channel the channel of the WebSocket connection.
     * @param frame   the close frame to send.
     * @param timeout the time to wait for the close frame of the client.
     * @return the future completed when the connection is closed.
     */
    static ChannelFuture start(Channel channel, CloseWebSocketFrame frame, Duration timeout) {
        if (channel.eventLoop().inEventLoop()) {
            startOnEventLoop(channel, frame, timeout);
        } else {
            try {
                channel.eventLoop().execute(() -> startOnEventLoop(channel, frame, timeout));
            } catch (RuntimeException exception) {
                frame.release();
                channel.close();
            }
        }
        return channel.closeFuture();
    }

    private static void startOnEventLoop(Channel channel, CloseWebSocketFrame frame, Duration timeout) {
        if (channel.attr(STATE).setIfAbsent(State.SERVER_STARTED) != null) {
            frame.release();
            return;
        }

        // Report what the frame carries to the close handler, unless the session is already being closed
        channel.attr(CloseInfo.KEY).setIfAbsent(new CloseInfo(frame.statusCode(), frame.reasonText()));

        closeAfter(channel, timeout);
        channel.writeAndFlush(frame).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    /**
     * Closes the connection after the timeout, unless it is closed before.
     *
     * @param channel the channel of the WebSocket connection.
     * @param timeout the time after which the connection is closed.
     */
    static void closeAfter(Channel channel, Duration timeout) {
        ScheduledFuture<?> closeTimeout = channel.eventLoop()
            .schedule(() -> channel.close(), timeout.toNanos(), TimeUnit.NANOSECONDS);
        channel.closeFuture().addListener(_ -> closeTimeout.cancel(false));
    }

    /**
     * Marks the connection as failed once a close frame other than the one of {@link #start} is written,
     * e.g. by Netty's validators, so nothing is sent after it (RFC 6455, section 7.1.7).
     *
     * @param channel the channel of the WebSocket connection.
     * @return true if this close frame is the first one, so the caller bounds the wait for the client's answer.
     */
    static boolean markFailed(Channel channel) {
        return channel.attr(STATE).setIfAbsent(State.SERVER_FAILED) == null;
    }

    static boolean markClientStarted(Channel channel) {
        return channel.attr(STATE).setIfAbsent(State.CLIENT_STARTED) == null;
    }

    static boolean isClosing(Channel channel) {
        return channel.attr(STATE).get() != null;
    }

    /**
     * Whether a Ping still gets its Pong. The closing handshake the server started gracefully with {@link #start}
     * keeps answering until the client's close frame arrives (RFC 6455, section 5.5.2), while a failed connection
     * and a connection whose client sent its close frame send nothing more.
     *
     * @param channel the channel of the WebSocket connection.
     * @return true if a Ping is answered.
     */
    static boolean answersPings(Channel channel) {
        State state = channel.attr(STATE).get();
        return state == null || state == State.SERVER_STARTED;
    }
}
