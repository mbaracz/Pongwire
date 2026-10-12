package pl.mbaracz.pongwire.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

class WebSocketServerMetricsTest {

    private MeterRegistry registry;
    private WebSocketServerMetrics<String, Object> metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new WebSocketServerMetrics<>(registry, Tags.of("server", "chat"));
    }

    @Test
    void shouldCountSessionsWhenSessionsAreOpenedAndClosed() {
        metrics.sessionOpened(null);
        metrics.sessionOpened(null);
        metrics.sessionOpened(null);
        metrics.sessionClosed(null, 1000, "bye");
        metrics.sessionClosed(null, 1006, "Abnormal closure");

        assertSoftly(softly -> {
            softly.assertThat(registry.get("pongwire.sessions.active").gauge().value()).as("Active sessions").isEqualTo(1);
            softly.assertThat(registry.get("pongwire.sessions.opened").counter().count()).as("Opened sessions").isEqualTo(3);
            softly.assertThat(registry.get("pongwire.sessions.closed").tag("code", "1000").counter().count()).as("Closed normally").isEqualTo(1);
            softly.assertThat(registry.get("pongwire.sessions.closed").tag("code", "1006").counter().count()).as("Closed abnormally").isEqualTo(1);
        });
    }

    @Test
    void shouldCountRejectedHandshakesByStatus() {
        metrics.handshakeRejected(401);
        metrics.handshakeRejected(401);
        metrics.handshakeRejected(400);

        assertSoftly(softly -> {
            softly.assertThat(registry.get("pongwire.handshakes.rejected").tag("status", "401").counter().count()).as("Unauthorized").isEqualTo(2);
            softly.assertThat(registry.get("pongwire.handshakes.rejected").tag("status", "400").counter().count()).as("Bad request").isEqualTo(1);
            softly.assertThat(registry.get("pongwire.handshakes.rejected").tag("server", "chat").counters()).as("Should be tagged").hasSize(2);
        });
    }

    @Test
    void shouldCountMessagesAndErrorsWhenTheyAreReported() {
        metrics.messageReceived(null);
        metrics.messageSent(null);
        metrics.messageSent(null);
        metrics.exception(null, new IllegalStateException());

        assertSoftly(softly -> {
            softly.assertThat(registry.get("pongwire.messages.received").counter().count()).as("Received messages").isEqualTo(1);
            softly.assertThat(registry.get("pongwire.messages.sent").counter().count()).as("Sent messages").isEqualTo(2);
            softly.assertThat(registry.get("pongwire.errors").counter().count()).as("Errors").isEqualTo(1);
        });
    }

    @Test
    void shouldTagEveryMetricWhenTagsAreGiven() {
        assertThat(registry.find("pongwire.sessions.active").tag("server", "chat").gauge()).as("Gauge should be tagged").isNotNull();
        assertThat(registry.find("pongwire.errors").tag("server", "chat").counter()).as("Counter should be tagged").isNotNull();
    }
}
