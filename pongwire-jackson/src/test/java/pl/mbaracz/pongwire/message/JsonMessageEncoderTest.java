package pl.mbaracz.pongwire.message;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.json.JsonMessageEncoder;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JsonMessageEncoderTest {

    private static class CyclicTestMessage {
        public String text;
        public CyclicTestMessage selfReference;

        public CyclicTestMessage(String text) {
            this.text = text;
            this.selfReference = this;
        }
    }

    private JsonMessageEncoder<Object> encoder;

    @BeforeEach
    void setUp() {
        encoder = new JsonMessageEncoder<>();
    }

    @Test
    void shouldEncodeMessageDecodableToOriginalMessageWhenMessageIsEncoded() throws IOException {
        // given
        TestMessage message = new TestMessage("hello", 123);
        ObjectMapper mapper = new ObjectMapper();

        // when
        byte[] encoded = encoder.encode(message);
        TestMessage decodedMessage = mapper.readValue(encoded, TestMessage.class);

        // then
        assertThat(decodedMessage).isEqualTo(message);
    }

    @Test
    void shouldThrowRuntimeExceptionWhenEncodingFails() {
        // given
        CyclicTestMessage message = new CyclicTestMessage("hello");

        // when / then
        assertThatThrownBy(() -> encoder.encode(message))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to encode message to JSON");
    }

    @Test
    void shouldUseCustomObjectMapperWithoutCopyingIt() {
        // given
        ObjectMapper mapper = new ObjectMapper();
        JsonMessageEncoder<TestMessage> customEncoder = new JsonMessageEncoder<>(mapper);
        SimpleModule module = new SimpleModule();
        module.addSerializer(TestMessage.class, new JsonSerializer<TestMessage>() {
            @Override
            public void serialize(TestMessage value, JsonGenerator generator, SerializerProvider serializers)
                throws IOException {
                generator.writeString(value.text.toUpperCase());
            }
        });
        mapper.registerModule(module);

        // when
        byte[] encoded = customEncoder.encode(new TestMessage("hello", 123));

        // then
        assertThat(encoded).isEqualTo("\"HELLO\"".getBytes());
    }

    @Test
    void shouldFailFastWhenObjectMapperIsNull() {
        assertThatThrownBy(() -> new JsonMessageEncoder<>(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("mapper");
    }
}
