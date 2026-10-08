package pl.mbaracz.jwebsockets.message;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.jwebsockets.message.impl.json.JsonMessageDecoder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JsonMessageDecoderTest {

    private JsonMessageDecoder<TestMessage> decoder;

    @BeforeEach
    void setUp() {
        decoder = new JsonMessageDecoder<>(TestMessage.class);
    }

    @Test
    void shouldReturnOriginalMessageWhenValidJsonIsDecoded() throws IOException {
        // given
        TestMessage originalMessage = new TestMessage("hello", 123);
        ObjectMapper mapper = new ObjectMapper();

        // when
        byte[] encoded = mapper.writeValueAsBytes(originalMessage);
        TestMessage decodedMessage = decoder.decode(encoded);

        // then
        assertThat(decodedMessage).isEqualTo(originalMessage);
    }

    @Test
    void shouldThrowMessageDecodeExceptionWhenEmptyJsonIsDecoded() {
        // given
        String emptyJson = "";

        // when / then
        assertThatThrownBy(() -> decoder.decode(emptyJson.getBytes()))
            .isInstanceOfSatisfying(
                MessageDecodeException.class,
                exception -> assertThat(exception.getPayload()).isEqualTo(emptyJson.getBytes())
            );
    }

    @Test
    void shouldDecodeGenericTypeFromTypeReference() {
        // given
        JsonMessageDecoder<List<TestMessage>> listDecoder = new JsonMessageDecoder<>(new TypeReference<>() {});

        // when
        List<TestMessage> messages = listDecoder.decode("[{\"text\":\"hello\",\"number\":123}]".getBytes());

        // then
        assertThat(messages).containsExactly(new TestMessage("hello", 123));
    }

    @Test
    void shouldDecodeGenericTypeFromJavaType() {
        // given
        ObjectMapper mapper = new ObjectMapper();
        JavaType type = mapper.getTypeFactory().constructMapType(Map.class, String.class, TestMessage.class);
        JsonMessageDecoder<Map<String, TestMessage>> mapDecoder = new JsonMessageDecoder<>(mapper, type);

        // when
        Map<String, TestMessage> messages = mapDecoder.decode(
            "{\"first\":{\"text\":\"hello\",\"number\":123}}".getBytes()
        );

        // then
        assertThat(messages).containsEntry("first", new TestMessage("hello", 123));
    }

    @Test
    void shouldUseCustomObjectMapperWithoutCopyingIt() {
        // given
        ObjectMapper mapper = new ObjectMapper();
        JsonMessageDecoder<TestMessage> customDecoder = new JsonMessageDecoder<>(mapper, TestMessage.class);
        SimpleModule module = new SimpleModule();
        module.addDeserializer(TestMessage.class, new JsonDeserializer<TestMessage>() {
            @Override
            public TestMessage deserialize(JsonParser parser, DeserializationContext context) throws IOException {
                return new TestMessage(parser.getValueAsString().toUpperCase(), 123);
            }
        });
        mapper.registerModule(module);

        // when
        TestMessage message = customDecoder.decode("\"hello\"".getBytes());

        // then
        assertThat(message).isEqualTo(new TestMessage("HELLO", 123));
    }

    @Test
    void shouldFailFastWhenConstructorArgumentIsNull() {
        ObjectMapper mapper = new ObjectMapper();

        assertThatThrownBy(() -> new JsonMessageDecoder<>(null, TestMessage.class))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("mapper");
        assertThatThrownBy(() -> new JsonMessageDecoder<>(mapper, (Class<TestMessage>) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("type");
        assertThatThrownBy(() -> new JsonMessageDecoder<>(mapper, (TypeReference<TestMessage>) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("type");
        assertThatThrownBy(() -> new JsonMessageDecoder<TestMessage>(mapper, (JavaType) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("type");
    }
}
