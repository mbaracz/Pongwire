package pl.mbaracz.pongwire.message.impl.plain;

import pl.mbaracz.pongwire.message.MessageDecoder;

import java.nio.charset.StandardCharsets;

/**
 * Decoder for plain text WebSocket messages.
 */
public class PlainTextMessageDecoder implements MessageDecoder<String> {

    /**
     * Singleton instance of PlainTextMessageDecoder.
     */
    public static final PlainTextMessageDecoder INSTANCE = new PlainTextMessageDecoder();

    /**
     * Decodes a byte array into a String using UTF-8 encoding.
     *
     * @param data the byte array containing the encoded message.
     * @return the decoded String message.
     */
    @Override
    public String decode(byte[] data) {
        return new String(data, StandardCharsets.UTF_8);
    }
}
