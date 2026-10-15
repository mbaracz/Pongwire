package pl.mbaracz.pongwire.message.impl.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import pl.mbaracz.pongwire.message.MessageDecodeException;
import pl.mbaracz.pongwire.message.MessageDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Decodes JSON messages of one type.
 * The input must be a single UTF-8 encoded JSON value other than {@code null} (RFC 8259, section 8.1).
 * Decoding runs on data from clients, so a mapper passed in must not enable polymorphic typing for untrusted classes,
 * e.g. {@code activateDefaultTyping} or {@code @JsonTypeInfo(use = Id.CLASS)}, which lets a client choose the classes
 * the server instantiates. Use a dedicated mapper rather than one shared with internal messaging or caching.
 *
 * @param <T> the type of messages.
 */
public class JsonMessageDecoder<T> implements MessageDecoder<T> {

    private final ObjectMapper mapper;

    private final JavaType type;

    /**
     * Creates a decoder for the given class with a default {@link ObjectMapper}, which also handles java.time types.
     *
     * @param clazz the class of messages to decode
     */
    public JsonMessageDecoder(Class<T> clazz) {
        this(DefaultObjectMapper.create(), clazz);
    }

    /**
     * Creates a decoder for the given class using the provided {@link ObjectMapper}.
     *
     * @param mapper the object mapper used to decode messages
     * @param type the class of messages to decode
     */
    public JsonMessageDecoder(ObjectMapper mapper, Class<T> type) {
        this(mapper, Objects.requireNonNull(mapper, "mapper").getTypeFactory().constructType(Objects.requireNonNull(type, "type")));
    }

    /**
     * Creates a decoder for the given generic type with a default {@link ObjectMapper}, which also handles java.time types.
     *
     * @param type the generic type of messages to decode
     */
    public JsonMessageDecoder(TypeReference<T> type) {
        this(DefaultObjectMapper.create(), type);
    }

    /**
     * Creates a decoder for the given generic type using the provided {@link ObjectMapper}.
     *
     * @param mapper the object mapper used to decode messages
     * @param type the generic type of messages to decode
     */
    public JsonMessageDecoder(ObjectMapper mapper, TypeReference<T> type) {
        this(mapper, Objects.requireNonNull(mapper, "mapper").getTypeFactory().constructType(Objects.requireNonNull(type, "type")));
    }

    /**
     * Creates a decoder for the given Jackson type with a default {@link ObjectMapper}, which also handles java.time types.
     *
     * @param type the Jackson type of messages to decode
     */
    public JsonMessageDecoder(JavaType type) {
        this(DefaultObjectMapper.create(), type);
    }

    /**
     * Creates a decoder for the given Jackson type using the provided {@link ObjectMapper}.
     *
     * @param mapper the object mapper used to decode messages
     * @param type the Jackson type of messages to decode
     */
    public JsonMessageDecoder(ObjectMapper mapper, JavaType type) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.type = Objects.requireNonNull(type, "type");
    }

    @Override
    public T decode(byte[] data) {
        if (!isUtf8(data)) {
            throw new MessageDecodeException("JSON message must be encoded in UTF-8", data, null);
        }

        T message;

        // Parsed with the mapper itself, so modules registered on it later still apply
        try (JsonParser parser = mapper.createParser(data)) {
            // The token is checked, because a deserializer may turn the null token into a value, e.g. a NullNode or 0
            if (parser.nextToken() == JsonToken.VALUE_NULL) {
                throw new MessageDecodeException("JSON null is not a message", data, null);
            }
            message = mapper.readValue(parser, type);

            if (parser.nextToken() != null) {
                throw new MessageDecodeException("JSON message must contain a single value", data, null);
            }
        } catch (IOException exception) {
            throw new MessageDecodeException("Failed to decode message from JSON", data, exception);
        }

        if (message == null) {
            throw new MessageDecodeException("JSON null is not a message", data, null);
        }
        return message;
    }

    /**
     * Checks that the bytes are well-formed UTF-8, with an optional byte order mark.
     * Jackson detects UTF-16 and UTF-32 from a byte order mark or from zero bytes at the start, which are valid
     * UTF-8 on their own, so a text in another encoding would be decoded instead of rejected.
     * Jackson also accepts overlong encodings, and a binary frame was not validated by the server like a text frame.
     */
    private static boolean isUtf8(byte[] data) {
        for (int i = 0; i < Math.min(4, data.length); i++) {
            if (data[i] == 0) {
                return false;
            }
        }

        if (data.length >= 2 && (data[0] == (byte) 0xFE && data[1] == (byte) 0xFF || data[0] == (byte) 0xFF && data[1] == (byte) 0xFE)) {
            return false;
        }

        int start = data.length >= 3 && data[0] == (byte) 0xEF && data[1] == (byte) 0xBB && data[2] == (byte) 0xBF ? 3 : 0;

        try {
            StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(data, start, data.length - start));
            return true;
        } catch (CharacterCodingException exception) {
            return false;
        }
    }
}
