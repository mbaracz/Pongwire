package pl.mbaracz.pongwire.configuration;

/**
 * Defines what happens to new messages while a connection is unwritable.
 */
public enum BackpressurePolicy {

    /**
     * Keeps accepting messages into Netty's outbound buffer. If an unwritable timeout is configured,
     * the connection is closed when it does not become writable before the timeout elapses.
     */
    BUFFER,

    /**
     * Rejects new messages until the connection becomes writable again.
     * A configured unwritable timeout still closes a connection that remains unwritable.
     */
    REJECT_NEW
}
