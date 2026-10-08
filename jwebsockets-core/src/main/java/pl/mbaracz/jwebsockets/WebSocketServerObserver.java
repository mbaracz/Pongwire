package pl.mbaracz.jwebsockets;

/**
 * Observes the events of a WebSocket server, e.g. to record metrics.
 * Exceptions thrown by the observer are logged and do not affect the session.
 *
 * @param <T> the type of the WebSocket message.
 * @param <D> the type of additional data associated with the WebSocket session.
 */
public interface WebSocketServerObserver<T, D> {

    /**
     * Called when a handshake request is answered with an HTTP error status instead of opening a session,
     * e.g. because the upgrade handler rejected it or the request is not a valid WebSocket handshake.
     *
     * @param status the HTTP status of the response.
     */
    default void handshakeRejected(int status) {
    }

    /**
     * Called when a session is opened, before the open handler.
     *
     * @param session the opened WebSocket session.
     */
    default void sessionOpened(WebSocketSession<T, D> session) {
    }

    /**
     * Called exactly once per opened session, after it was removed from the server and before the close handler.
     *
     * @param session the closed WebSocket session.
     * @param code    the close status code, as reported to the close handler.
     * @param reason  the close reason, as reported to the close handler.
     */
    default void sessionClosed(WebSocketSession<T, D> session, int code, String reason) {
    }

    /**
     * Called when a message was received and decoded, before the message handler.
     *
     * @param session the WebSocket session that received the message.
     */
    default void messageReceived(WebSocketSession<T, D> session) {
    }

    /**
     * Called when a message was successfully written to the connection.
     * Failed writes are reported through {@link #exception(WebSocketSession, Throwable)} instead.
     *
     * @param session the WebSocket session the message was sent to.
     */
    default void messageSent(WebSocketSession<T, D> session) {
    }

    /**
     * Called when an exception was caught on a connection, including
     * while decoding, handling, encoding, or writing a message.
     *
     * @param session   the WebSocket session, or null if the exception occurred before the session was opened.
     * @param exception the caught exception.
     */
    default void exception(WebSocketSession<T, D> session, Throwable exception) {
    }
}
