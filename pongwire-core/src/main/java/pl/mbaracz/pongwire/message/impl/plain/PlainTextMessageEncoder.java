package pl.mbaracz.pongwire.message.impl.plain;

import pl.mbaracz.pongwire.message.MessageEncoder;

import java.nio.charset.StandardCharsets;

/**
 * Encoder for plain text WebSocket messages.
 */
public class PlainTextMessageEncoder implements MessageEncoder<String> {

    /**
     * Singleton instance of PlainTextMessageEncoder.
     */
    public static final PlainTextMessageEncoder INSTANCE = new PlainTextMessageEncoder();

    /**
     * Encodes a String message into a byte array using UTF-8 encoding.
     *
     * @param message the String message to be encoded.
     * @return the byte array containing the encoded message.
     */
    @Override
    public byte[] encode(String message) {
        return message.getBytes(StandardCharsets.UTF_8);
    }
}
