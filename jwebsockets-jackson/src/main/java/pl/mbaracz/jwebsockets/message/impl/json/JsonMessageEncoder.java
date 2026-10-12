package pl.mbaracz.jwebsockets.message.impl.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import pl.mbaracz.jwebsockets.message.MessageEncoder;

import java.util.Objects;

public class JsonMessageEncoder<T> implements MessageEncoder<T> {

    private final ObjectMapper mapper;

    /**
     * Creates an encoder with a default {@link ObjectMapper}, which also handles java.time types.
     */
    public JsonMessageEncoder() {
        this(DefaultObjectMapper.create());
    }

    /**
     * Creates an encoder that uses the provided {@link ObjectMapper}.
     *
     * @param mapper the object mapper used to encode messages
     */
    public JsonMessageEncoder(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public byte[] encode(T message) {
        try {
            return mapper.writeValueAsBytes(message);
        } catch (JsonProcessingException exception) {
            throw new RuntimeException("Failed to encode message to JSON", exception);
        }
    }
}
