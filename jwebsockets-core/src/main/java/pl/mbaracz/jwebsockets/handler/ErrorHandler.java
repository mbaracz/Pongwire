package pl.mbaracz.jwebsockets.handler;

import pl.mbaracz.jwebsockets.WebSocketSession;
import pl.mbaracz.jwebsockets.message.MessageDecodeException;

/**
 * Handles application errors reported by a WebSocket connection.
 * Exceptions thrown by the handler are logged and ignored, so they do not affect the configured close policy.
 *
 * @param <T> the type of the WebSocket message.
 * @param <D> the type of the WebSocket session context.
 */
@FunctionalInterface
public interface ErrorHandler<T, D> {

    /**
     * Handles an error reported by a WebSocket connection.
     *
     * @param session   the WebSocket session, or null if the error occurred before the session was opened.
     * @param exception the reported error, a {@link MessageDecodeException} with the payload
     *                  if a received message could not be decoded.
     */
    void handle(WebSocketSession<T, D> session, Throwable exception);
}
