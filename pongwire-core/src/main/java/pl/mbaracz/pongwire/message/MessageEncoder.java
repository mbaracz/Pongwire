package pl.mbaracz.pongwire.message;

/**
 * Interface for encoding WebSocket messages into byte arrays.
 *
 * @param <T> the type of the WebSocket message to be encoded.
 */
@FunctionalInterface
public interface MessageEncoder<T> {

    /**
     * Encodes a WebSocket message into a byte array.
     * A broadcast or a topic message is encoded once and the array is shared by every session,
     * so return a new array and do not change it later. For text frames it must be UTF-8.
     *
     * @param message the WebSocket message to be encoded.
     * @return the byte array containing the encoded message.
     */
    byte[] encode(T message);

}
