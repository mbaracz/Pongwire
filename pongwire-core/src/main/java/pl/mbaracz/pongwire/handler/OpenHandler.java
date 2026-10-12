package pl.mbaracz.pongwire.handler;

import pl.mbaracz.pongwire.WebSocketSession;

/**
 * Interface for handling the opening of a new WebSocket connection after a successful upgrade.
 *
 * @param <T> the type of the WebSocket message.
 * @param <D> the type of additional data associated with the WebSocket session.
 */
@FunctionalInterface
public interface OpenHandler<T, D> {

    /**
     * Handles the opening of a new WebSocket session after a successful upgrade.
     *
     * @param session the newly opened WebSocket session.
     */
    void handleOpen(WebSocketSession<T, D> session);

}
