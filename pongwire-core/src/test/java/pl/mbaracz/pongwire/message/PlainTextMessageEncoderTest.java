package pl.mbaracz.pongwire.message;

import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import static org.assertj.core.api.Assertions.assertThat;

class PlainTextMessageEncoderTest {

    @Test
    void shouldReturnOriginalMessageWhenEncodedMessageIsDecoded() {
        // Given
        String message = "hello";

        // When
        byte[] encoded = PlainTextMessageEncoder.INSTANCE.encode(message);

        // Then
        assertThat(PlainTextMessageDecoder.INSTANCE.decode(encoded)).isEqualTo(message);
    }
}
