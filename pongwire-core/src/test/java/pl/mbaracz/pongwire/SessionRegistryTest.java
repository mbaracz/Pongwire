package pl.mbaracz.pongwire;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class SessionRegistryTest {

    private SessionRegistry<String, Object> registry;

    @BeforeEach
    void setUp() {
        registry = new SessionRegistry<>();
    }

    @Test
    void shouldRefuseSessionRegisteredAfterStopAccepting() {
        WebSocketSession<String, Object> before = newSession();
        WebSocketSession<String, Object> after = newSession();

        assertThat(registry.register(before)).as("Registration while accepting").isTrue();
        assertThat(registry.stopAccepting()).as("Sessions to close").containsExactly(before);
        assertThat(registry.register(after)).as("Registration after stop").isFalse();
        assertThat(registry.contains(after)).as("Refused session should not be registered").isFalse();
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void shouldAcceptSessionsAgainWhenAcceptingIsStarted() {
        registry.stopAccepting();
        registry.startAccepting();

        assertThat(registry.register(newSession())).as("Registration after restart").isTrue();
    }

    private static WebSocketSession<String, Object> newSession() {
        EmbeddedChannel channel = Util.newEmbeddedChannel(new ChannelInboundHandlerAdapter());
        return new WebSocketSession<>(channel.pipeline().firstContext(), (_, _) -> null, _ -> new byte[0], null, Duration.ofSeconds(1));
    }
}
