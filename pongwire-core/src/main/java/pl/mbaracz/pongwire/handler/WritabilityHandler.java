package pl.mbaracz.pongwire.handler;

import pl.mbaracz.pongwire.WebSocketSession;

/**
 * Interface for handling changes of WebSocket session writability.
 *
 * @param <T> the type of the WebSocket message.
 * @param <D> the type of additional data associated with the WebSocket session.
 */
@FunctionalInterface
public interface WritabilityHandler<T, D> {

    /**
     * Handles a change of the session's writability.
     * Changes while an earlier call waits for the callback executor are merged,
     * so the handler receives the current writability and never the same value twice in a row.
     *
     * @param session  the WebSocket session whose writability changed.
     * @param writable whether the session is writable now.
     */
    void handleWritabilityChanged(WebSocketSession<T, D> session, boolean writable);

}
