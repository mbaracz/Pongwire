package pl.mbaracz.pongwire.message;

/**
 * Interface for decoding WebSocket messages from byte arrays.
 *
 * @param <T> the type of the decoded WebSocket message.
 */
@FunctionalInterface
public interface MessageDecoder<T> {

    /**
     * Decodes a WebSocket message from a byte array.
     *
     * @param data the byte array containing the encoded message.
     * @return the decoded WebSocket message.
     * @throws MessageDecodeException if the data is not a valid message. The server wraps other exceptions in one.
     */
    T decode(byte[] data);

}
