package pl.mbaracz.jwebsockets.handler;

import pl.mbaracz.jwebsockets.WebSocketSession;

/**
 * Interface for handling WebSocket connection closure events.
 *
 * @param <T> the type of the WebSocket message.
 * @param <D> the type of additional data associated with the WebSocket session.
 */
@FunctionalInterface
public interface CloseHandler<T, D> {

    /**
     * Handles the closure of a WebSocket session.
     * Called exactly once per opened session, after the session was removed from the server and its topics.
     *
     * @param session the WebSocket session that is being closed.
     * @param reason  the reason for the WebSocket connection closure.
     * @param code    the status code of the close frame received from the client, 1005 if that frame had no status code,
     *                1001 if the server closed the session because it is stopping, heartbeat timed out or it was idle,
     *                the code passed to {@link WebSocketSession#close(int, String)} when the application closed it,
     *                1007 or 1011 if the server closed it on an exception with closeOnException enabled,
     *                or 1006 if the connection was closed without a close frame (RFC 6455, section 7.1.5).
     */
    void handleClose(WebSocketSession<T, D> session, String reason, int code);

}
