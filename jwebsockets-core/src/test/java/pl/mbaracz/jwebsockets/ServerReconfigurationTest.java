package pl.mbaracz.jwebsockets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.jwebsockets.configuration.BackpressurePolicy;
import pl.mbaracz.jwebsockets.configuration.WebSocketServerConfiguration;
import pl.mbaracz.jwebsockets.handler.UpgradeResult;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

class ServerReconfigurationTest {

    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            );
    }

    @Test
    void shouldUseBufferBackpressurePolicyByDefault() {
        assertThat(server.getConfiguration().getBackpressurePolicy()).isEqualTo(BackpressurePolicy.BUFFER);
    }

    @Test
    void shouldUseDefaultSizeLimits() {
        assertSoftly(softly -> {
            softly.assertThat(server.getConfiguration().getMaxHandshakeHeaderSize()).isEqualTo(8 * 1024);
            softly.assertThat(server.getConfiguration().getMaxFrameSize()).isEqualTo(1024 * 1024);
            softly.assertThat(server.getConfiguration().getMaxMessageSize()).isEqualTo(1024 * 1024);
        });
    }

    @Test
    void shouldThrowOnConfigureWhenServerIsRunning() {
        server.listen(0);

        try {
            assertThatThrownBy(() -> server.configure(configurer -> configurer.setRespondWithBinaryFrame(true)))
                .isInstanceOf(IllegalStateException.class);
            assertThat(server.getConfiguration().isRespondWithBinaryFrame()).as("Configuration should not change").isFalse();
        } finally {
            server.stop();
        }
    }

    @Test
    void shouldThrowOnSettingHandlersWhenServerIsRunning() {
        server.listen(0);

        try {
            assertSoftly(softly -> {
                softly.assertThatThrownBy(() -> server.onMessage((_, _) -> {})).isInstanceOf(IllegalStateException.class);
                softly.assertThatThrownBy(() -> server.onError((_, _) -> {})).isInstanceOf(IllegalStateException.class);
                softly.assertThatThrownBy(() -> server.onOpen(_ -> {})).isInstanceOf(IllegalStateException.class);
                softly.assertThatThrownBy(() -> server.onClose((_, _, _) -> {})).isInstanceOf(IllegalStateException.class);
                softly.assertThatThrownBy(() -> server.onUpgrade((_, _) -> UpgradeResult.accept(null))).isInstanceOf(IllegalStateException.class);
                softly.assertThatThrownBy(() -> server.onWritabilityChanged((_, _) -> {})).isInstanceOf(IllegalStateException.class);
                softly.assertThatThrownBy(() -> server.topicBroker(new InMemoryTopicBroker<>())).isInstanceOf(IllegalStateException.class);
            });
        } finally {
            server.stop();
        }
    }

    @Test
    void shouldAllowReconfigurationAndRestartWhenServerIsStopped() {
        server.listen(0);
        server.stop();

        // Reconfigure the stopped server before starting it again
        server.configure(configurer -> configurer.setRespondWithBinaryFrame(true))
            .onMessage((_, _) -> {});
        server.listen(0);

        try {
            assertThat(server.isRunning()).as("Server should be running").isTrue();
            assertThat(server.getConfiguration().isRespondWithBinaryFrame()).as("Configuration should change").isTrue();
        } finally {
            server.stop();
        }
    }

    @Test
    void shouldNotChangeServerConfigurationWhenBuilderIsModifiedAfterConfigure() {
        AtomicReference<WebSocketServerConfiguration.Builder<String>> captured = new AtomicReference<>();
        server.configure(captured::set);

        // Change the builder kept from configure(), before and while the server is running
        captured.get().setMaxMessageSize(123);
        server.listen(0);

        try {
            captured.get().setMaxMessageSize(456);

            assertThat(server.getConfiguration().getMaxMessageSize())
                .as("Server should keep the configuration built by configure()")
                .isEqualTo(1024 * 1024);
        } finally {
            server.stop();
        }
    }

    @Test
    void shouldNotChangeActiveConfigurationWhenAllowedOriginsListIsModifiedWhileRunning() {
        List<String> origins = new ArrayList<>(List.of("http://example.com"));
        server.configure(configurer -> configurer.setAllowedOrigins(origins));
        server.listen(0);

        try {
            // Change the list passed to setAllowedOrigins()
            origins.add("http://other.example.com");

            assertThat(server.getConfiguration().getAllowedOrigins())
                .as("Running server should keep its allowed origins")
                .isEqualTo(List.of("http://example.com"));
        } finally {
            server.stop();
        }
    }

    @Test
    void shouldNotChangeConfigurationWhenAllowedOriginsListIsModifiedBeforeStartup() {
        List<String> origins = new ArrayList<>(List.of("http://example.com"));
        server.configure(configurer -> configurer.setAllowedOrigins(origins));

        // Change the list passed to setAllowedOrigins() before the server is started
        origins.add("http://other.example.com");

        assertThat(server.getConfiguration().getAllowedOrigins())
            .as("Configuration should keep its allowed origins")
            .isEqualTo(List.of("http://example.com"));
    }

    @Test
    void shouldKeepOtherSettingsWhenConfigureIsCalledAgain() {
        TlsConfiguration tlsConfiguration = TlsConfiguration.forPem(Path.of("certificate.pem"), Path.of("key.pem"));
        Pattern originPattern = Pattern.compile("^https://example\\.com$");
        Executor callbackExecutor = Runnable::run;

        server.configure(configurer -> configurer
            .setAllowTextFrames(false)
            .setAllowBinaryFrames(true)
            .setRespondWithBinaryFrame(true)
            .setTlsConfiguration(tlsConfiguration)
            .setCloseOnException(true)
            .setMaxHandshakeHeaderSize(4096)
            .setMaxFrameSize(1024)
            .setHandshakeTimeout(Duration.ofSeconds(7))
            .setHeartbeatInterval(Duration.ofSeconds(30))
            .setHeartbeatTimeout(Duration.ofSeconds(5))
            .setIdleTimeout(Duration.ofMinutes(5))
            .setCloseTimeout(Duration.ofSeconds(3))
            .setCallbackExecutor(callbackExecutor)
            .setWriteBufferWaterMark(1024, 2048)
            .setBackpressurePolicy(BackpressurePolicy.REJECT_NEW)
            .setUnwritableTimeout(Duration.ofSeconds(20))
            .setAllowedOrigins("https://example.com")
            .setAllowedOriginPattern(originPattern)
            .setSubprotocols("chat")
        );

        // Change one setting only
        server.configure(configurer -> configurer.setMaxMessageSize(2048));

        WebSocketServerConfiguration<String> configuration = server.getConfiguration();

        assertThat(configuration.getMaxMessageSize()).as("Changed setting should be applied").isEqualTo(2048);
        assertSoftly(softly -> {
            softly.assertThat(configuration.isAllowTextFrames()).as("allowTextFrames").isFalse();
            softly.assertThat(configuration.isAllowBinaryFrames()).as("allowBinaryFrames").isTrue();
            softly.assertThat(configuration.isRespondWithBinaryFrame()).as("respondWithBinaryFrame").isTrue();
            softly.assertThat(configuration.getTlsConfiguration()).as("tlsConfiguration").isSameAs(tlsConfiguration);
            softly.assertThat(configuration.isCloseOnException()).as("closeOnException").isTrue();
            softly.assertThat(configuration.getMaxHandshakeHeaderSize()).as("maxHandshakeHeaderSize").isEqualTo(4096);
            softly.assertThat(configuration.getMaxFrameSize()).as("maxFrameSize").isEqualTo(1024);
            softly.assertThat(configuration.getHandshakeTimeout()).as("handshakeTimeout").isEqualTo(Duration.ofSeconds(7));
            softly.assertThat(configuration.getHeartbeatInterval()).as("heartbeatInterval").isEqualTo(Duration.ofSeconds(30));
            softly.assertThat(configuration.getHeartbeatTimeout()).as("heartbeatTimeout").isEqualTo(Duration.ofSeconds(5));
            softly.assertThat(configuration.getIdleTimeout()).as("idleTimeout").isEqualTo(Duration.ofMinutes(5));
            softly.assertThat(configuration.getCloseTimeout()).as("closeTimeout").isEqualTo(Duration.ofSeconds(3));
            softly.assertThat(configuration.getCallbackExecutor()).as("callbackExecutor").isSameAs(callbackExecutor);
            softly.assertThat(configuration.getWriteBufferLowWaterMark()).as("writeBufferLowWaterMark").isEqualTo(1024);
            softly.assertThat(configuration.getWriteBufferHighWaterMark()).as("writeBufferHighWaterMark").isEqualTo(2048);
            softly.assertThat(configuration.getBackpressurePolicy()).as("backpressurePolicy").isEqualTo(BackpressurePolicy.REJECT_NEW);
            softly.assertThat(configuration.getUnwritableTimeout()).as("unwritableTimeout").isEqualTo(Duration.ofSeconds(20));
            softly.assertThat(configuration.getAllowedOrigins()).as("allowedOrigins").isEqualTo(List.of("https://example.com"));
            softly.assertThat(configuration.getAllowedOriginPattern()).as("allowedOriginPattern").isSameAs(originPattern);
            softly.assertThat(configuration.getSubprotocols()).as("subprotocols").isEqualTo(List.of("chat"));
            softly.assertThat(configuration.getMessageEncoder()).as("messageEncoder").isSameAs(PlainTextMessageEncoder.INSTANCE);
            softly.assertThat(configuration.getMessageDecoder()).as("messageDecoder").isSameAs(PlainTextMessageDecoder.INSTANCE);
        });
    }

    @Test
    void shouldUseLastBuiltConfigurationWhenServerStarts() {
        server
            .configure(configurer -> configurer
                .setMaxFrameSize(2048)
                .setMaxMessageSize(2048)
            )
            .configure(configurer -> configurer.setSubprotocols("chat"));
        WebSocketServerConfiguration<String> built = server.getConfiguration();

        server.listen(0);

        try {
            assertThat(server.getConfiguration()).as("Server should run with the last built configuration").isSameAs(built);
        } finally {
            server.stop();
        }
    }

    @Test
    void shouldRejectFrameLimitLargerThanMessageLimit() {
        assertThatThrownBy(() -> server.configure(configurer -> configurer
            .setMaxFrameSize(2048)
            .setMaxMessageSize(1024)
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Maximum frame size must not exceed maximum message size!");

        assertThat(server.getConfiguration().getMaxFrameSize()).isEqualTo(1024 * 1024);
        assertThat(server.getConfiguration().getMaxMessageSize()).isEqualTo(1024 * 1024);
    }

    @Test
    void shouldThrowWithoutChangingConfigurationWhenSettingIsInvalid() {
        // The heartbeat interval is set before the invalid size, but the configuration is never built
        assertThatThrownBy(() -> server.configure(configurer -> configurer
            .setHeartbeatInterval(Duration.ofSeconds(30))
            .setMaxMessageSize(0)
        )).isInstanceOf(IllegalArgumentException.class);

        assertThat(server.getConfiguration().getHeartbeatInterval()).as("Configuration should not change").isNull();
        assertSoftly(softly -> {
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setHeartbeatInterval(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setMaxHandshakeHeaderSize(0)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setMaxFrameSize(0)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setMaxMessageSize(0)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setMaxHandshakeHeaderSize(-1)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setMaxFrameSize(-1)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setMaxMessageSize(-1)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setHandshakeTimeout(null)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setHandshakeTimeout(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setHeartbeatTimeout(null)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setIdleTimeout(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setCloseTimeout(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setUnwritableTimeout(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setWriteBufferWaterMark(2048, 1024)))
                .isInstanceOf(IllegalArgumentException.class);
            softly.assertThatThrownBy(() -> server.configure(configurer -> configurer.setBackpressurePolicy(null)))
                .isInstanceOf(NullPointerException.class);
        });
    }
}
