package pl.mbaracz.pongwire.handler;

import pl.mbaracz.pongwire.WebSocketSession;

/**
 * Interface for handling incoming WebSocket messages.
 *
 * @param <T> the type of the WebSocket message.
 * @param <D> the type of additional data associated with the WebSocket session.
 */
@FunctionalInterface
public interface MessageHandler<T, D> {

    /**
     * Handles an incoming WebSocket message.
     *
     * @param session the WebSocket session from which the message was received.
     * @param message the WebSocket message that was received.
     */
    void handleMessage(WebSocketSession<T, D> session, T message);

}
