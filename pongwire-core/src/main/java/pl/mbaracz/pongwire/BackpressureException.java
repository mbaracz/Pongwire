package pl.mbaracz.pongwire;

/**
 * Indicates that a message was rejected because the WebSocket session is currently unwritable.
 */
public final class BackpressureException extends RuntimeException {

    public BackpressureException() {
        super("WebSocket session is not writable");
    }
}
