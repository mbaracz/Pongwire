package pl.mbaracz.jwebsockets.message.impl.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import pl.mbaracz.jwebsockets.message.MessageDecodeException;
import pl.mbaracz.jwebsockets.message.MessageDecoder;

import java.io.IOException;
import java.util.Objects;

public class JsonMessageDecoder<T> implements MessageDecoder<T> {

    private final ObjectMapper mapper;

    private final JavaType type;

    /**
     * Creates a decoder for the given class with a default {@link ObjectMapper}.
     *
     * @param clazz the class of messages to decode
     */
    public JsonMessageDecoder(Class<T> clazz) {
        this(new ObjectMapper(), clazz);
    }

    /**
     * Creates a decoder for the given class using the provided {@link ObjectMapper}.
     *
     * @param mapper the object mapper used to decode messages
     * @param type the class of messages to decode
     */
    public JsonMessageDecoder(ObjectMapper mapper, Class<T> type) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.type = mapper.getTypeFactory().constructType(Objects.requireNonNull(type, "type"));
    }

    /**
     * Creates a decoder for the given generic type with a default {@link ObjectMapper}.
     *
     * @param type the generic type of messages to decode
     */
    public JsonMessageDecoder(TypeReference<T> type) {
        this(new ObjectMapper(), type);
    }

    /**
     * Creates a decoder for the given generic type using the provided {@link ObjectMapper}.
     *
     * @param mapper the object mapper used to decode messages
     * @param type the generic type of messages to decode
     */
    public JsonMessageDecoder(ObjectMapper mapper, TypeReference<T> type) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.type = mapper.getTypeFactory().constructType(Objects.requireNonNull(type, "type"));
    }

    /**
     * Creates a decoder for the given Jackson type with a default {@link ObjectMapper}.
     *
     * @param type the Jackson type of messages to decode
     */
    public JsonMessageDecoder(JavaType type) {
        this(new ObjectMapper(), type);
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
        try {
            return mapper.readValue(data, type);
        } catch (IOException exception) {
            throw new MessageDecodeException("Failed to decode message from JSON", data, exception);
        }
    }
}
