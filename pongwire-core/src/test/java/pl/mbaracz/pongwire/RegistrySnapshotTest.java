package pl.mbaracz.pongwire;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RegistrySnapshotTest {

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
    void shouldNotChangeReturnedTopicsWhenSubscriptionsChange() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());

        server.subscribe(session, "first");
        Set<String> topics = server.getTopics();

        // Change the subscriptions after the topics were returned
        server.subscribe(session, "second");
        server.unsubscribe(session, "first");

        // Assert returned topics are a snapshot, while a new call sees the change
        assertThat(topics).as("Returned topics should not change").isEqualTo(Set.of("first"));
        assertThat(server.getTopics()).as("New call should return current topics").isEqualTo(Set.of("second"));
    }

    @Test
    void shouldNotChangeSubscriptionsWhenReturnedTopicsAreModified() {
        EmbeddedChannel channel = Util.connect(server);
        WebSocketSession<String, Object> session = server.getSessionByChannelId(channel.id());

        server.subscribe(session, "topic");

        // Try to remove the topic through the returned set
        assertThatThrownBy(() -> server.getTopics().remove("topic")).isInstanceOf(UnsupportedOperationException.class);

        // Assert subscription was not removed
        assertThat(server.isSubscribed(session, "topic")).as("Session should stay subscribed").isTrue();
    }

    @Test
    void shouldNotChangeReturnedSessionsWhenSessionsConnectOrDisconnect() {
        List<WebSocketSession<String, Object>> opened = new ArrayList<>();
        server.onOpen(opened::add);

        EmbeddedChannel firstChannel = Util.connect(server);
        Collection<WebSocketSession<String, Object>> sessions = server.getConnectedSessions();

        // Disconnect the first session and connect a second one after the sessions were returned.
        // Embedded channels share one channel id, so both sessions must not be connected at the same time.
        firstChannel.close();
        Util.connect(server);

        // Assert returned sessions are a snapshot, while a new call sees the change
        assertThat(List.copyOf(sessions)).as("Returned sessions should not change").isEqualTo(List.of(opened.getFirst()));
        assertThat(List.copyOf(server.getConnectedSessions())).as("New call should return current sessions").isEqualTo(List.of(opened.getLast()));
    }
}
