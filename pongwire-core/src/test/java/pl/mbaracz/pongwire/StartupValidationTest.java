package pl.mbaracz.pongwire;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StartupValidationTest {

    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<>();
    }

    @Test
    void shouldThrowOnListenWhenMessageEncoderIsMissing() {
        server.configure(configurer -> configurer.setMessageDecoder(PlainTextMessageDecoder.INSTANCE));

        assertThatThrownBy(() -> server.listen(0))
            .isInstanceOf(IllegalStateException.class)
            .as("Should be rejected because of the missing encoder")
            .hasMessageContaining("encoder");
        assertThat(server.isRunning()).as("Server should not be running").isFalse();
    }

    @Test
    void shouldThrowOnListenWhenMessageDecoderIsMissing() {
        server.configure(configurer -> configurer.setMessageEncoder(PlainTextMessageEncoder.INSTANCE));

        assertThatThrownBy(() -> server.listen(0))
            .isInstanceOf(IllegalStateException.class)
            .as("Should be rejected because of the missing decoder")
            .hasMessageContaining("decoder");
        assertThat(server.isRunning()).as("Server should not be running").isFalse();
    }

    @Test
    void shouldAllowConfigurationAndStartupWhenStartupFailedValidation() {
        server.configure(configurer -> configurer.setMessageDecoder(PlainTextMessageDecoder.INSTANCE));

        assertThatThrownBy(() -> server.listen(0)).isInstanceOf(IllegalStateException.class);

        // Complete the configuration and start the server again
        server.configure(configurer -> configurer.setMessageEncoder(PlainTextMessageEncoder.INSTANCE));
        server.listen(0);

        try {
            assertThat(server.isRunning()).as("Server should be running").isTrue();
        } finally {
            server.stop();
        }
    }
}
