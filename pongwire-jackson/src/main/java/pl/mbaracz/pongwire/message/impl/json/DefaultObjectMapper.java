package pl.mbaracz.pongwire.message.impl.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * The mapper the codecs use when the application passes none.
 */
final class DefaultObjectMapper {

    private DefaultObjectMapper() {
    }

    /**
     * Creates a mapper with Jackson's defaults and java.time support, e.g. for {@code Instant} fields.
     * Unknown properties still fail decoding.
     */
    static ObjectMapper create() {
        return JsonMapper.builder().addModule(new JavaTimeModule()).build();
    }
}
