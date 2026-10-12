package pl.mbaracz.pongwire;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TlsConfigurationTest {

    private static final Path CERTIFICATE_CHAIN = Path.of("certificate.pem");
    private static final Path PRIVATE_KEY = Path.of("private-key.pem");

    @Test
    void shouldCreateConfigurationForUnencryptedPemPrivateKey() {
        TlsConfiguration configuration = TlsConfiguration.forPem(CERTIFICATE_CHAIN, PRIVATE_KEY);

        assertThat(configuration.certificateChain()).isEqualTo(CERTIFICATE_CHAIN);
        assertThat(configuration.privateKey()).isEqualTo(PRIVATE_KEY);
        assertThat(configuration.privateKeyPassword()).isNull();
    }

    @Test
    void shouldCreateConfigurationForEncryptedPemPrivateKey() {
        TlsConfiguration configuration = TlsConfiguration.forPem(CERTIFICATE_CHAIN, PRIVATE_KEY, "secret".toCharArray());

        assertThat(configuration.certificateChain()).isEqualTo(CERTIFICATE_CHAIN);
        assertThat(configuration.privateKey()).isEqualTo(PRIVATE_KEY);
        assertThat(configuration.privateKeyPassword()).isEqualTo("secret");
    }

    @Test
    void shouldRequireCertificateAndPrivateKeyPaths() {
        assertThatNullPointerException().isThrownBy(() -> TlsConfiguration.forPem(null, PRIVATE_KEY));
        assertThatNullPointerException().isThrownBy(() -> TlsConfiguration.forPem(CERTIFICATE_CHAIN, null));
    }

    @Test
    void shouldFailListenWhenPrivateKeyDoesNotMatchCertificate() throws Exception {
        SelfSignedCertificate first = new SelfSignedCertificate("localhost");
        SelfSignedCertificate second = new SelfSignedCertificate("localhost");
        WebSocketServer<String, Object> server = newServer(TlsConfiguration.forPem(first.certificate().toPath(), second.privateKey().toPath()));

        try {
            assertThatThrownBy(() -> server.listen(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match");
            assertThat(server.isRunning()).as("Server should not be running").isFalse();
        } finally {
            first.delete();
            second.delete();
        }
    }

    @Test
    void shouldNameTheFilesWhenTheyCannotBeRead() {
        WebSocketServer<String, Object> server = newServer(TlsConfiguration.forPem(Path.of("missing-certificate.pem"), PRIVATE_KEY));

        assertThatThrownBy(() -> server.listen(0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("missing-certificate.pem");
    }

    private static WebSocketServer<String, Object> newServer(TlsConfiguration configuration) {
        return new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                .setTlsConfiguration(configuration)
            );
    }
}
