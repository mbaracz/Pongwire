package pl.mbaracz.jwebsockets.message;

/**
 * Indicates that a received message could not be decoded.
 * The server reports it to the error handler with the payload of the message,
 * and closes the session with 1007 if closeOnException is enabled.
 */
public final class MessageDecodeException extends RuntimeException {

    private final byte[] payload;

    /**
     * Creates the exception for a message that could not be decoded.
     *
     * @param message the detail message.
     * @param payload the payload of the received message.
     * @param cause   the failure of the decoder, or null.
     */
    public MessageDecodeException(String message, byte[] payload, Throwable cause) {
        super(message, cause);
        this.payload = payload;
    }

    /**
     * Returns the payload of the message that could not be decoded.
     *
     * @return the payload of the received message.
     */
    public byte[] getPayload() {
        return payload;
    }
}
