package pl.mbaracz.jwebsockets;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ServerObserverTest {

    private final List<String> events = new ArrayList<>();
    private final List<WebSocketSession<String, Object>> opened = new ArrayList<>();
    private final List<String> messages = new ArrayList<>();
    private WebSocketServer<String, Object> server;

    // Records the events in the order the server reports them
    private class RecordingObserver implements WebSocketServerObserver<String, Object> {

        @Override
        public void sessionOpened(WebSocketSession<String, Object> session) {
            events.add("opened");
        }

        @Override
        public void sessionClosed(WebSocketSession<String, Object> session, int code, String reason) {
            events.add("closed " + code + " " + reason);
        }

        @Override
        public void messageReceived(WebSocketSession<String, Object> session) {
            events.add("received");
        }

        @Override
        public void messageSent(WebSocketSession<String, Object> session) {
            events.add("sent");
        }

        @Override
        public void exception(WebSocketSession<String, Object> session, Throwable exception) {
            events.add("exception " + exception.getMessage());
        }
    }

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            )
            .onOpen(opened::add)
            .onMessage((_, message) -> messages.add(message))
            .observer(new RecordingObserver());
    }

    @Test
    void shouldReportSessionLifecycleAndMessagesWhenClientExchangesMessages() {
        EmbeddedChannel channel = Util.connect(server);

        Util.sendFromClient(channel, new TextWebSocketFrame("hello"));
        opened.getFirst().sendMessage("world");
        Util.sendFromClient(channel, new CloseWebSocketFrame(1000, "bye"));

        assertThat(events).as("Observer should see every event in order")
            .containsExactly("opened", "received", "sent", "closed 1000 bye");
    }

    @Test
    void shouldReportSessionClosedOnceWhenConnectionDropsAfterCloseFrame() {
        EmbeddedChannel channel = Util.connect(server);

        Util.sendFromClient(channel, new CloseWebSocketFrame(1000, "bye"));
        channel.close();

        assertThat(events).as("Session should be reported closed once").containsExactly("opened", "closed 1000 bye");
    }

    @Test
    void shouldReportExceptionWhenMessageCannotBeDecoded() {
        server.configure(configurer -> configurer.setMessageDecoder(_ -> {
            throw new IllegalArgumentException("malformed");
        }));
        EmbeddedChannel channel = Util.connect(server);

        Util.sendFromClient(channel, new TextWebSocketFrame("hello"));

        assertThat(events).as("Exception should be reported instead of the message")
            .containsExactly("opened", "exception Failed to decode message");
    }

    @Test
    void shouldKeepSessionOpenWhenObserverThrows() {
        server
            .configure(configurer -> configurer.setCloseOnException(true))
            .observer(new WebSocketServerObserver<>() {
                @Override
                public void messageReceived(WebSocketSession<String, Object> session) {
                    throw new IllegalStateException("observer failed");
                }
            });
        EmbeddedChannel channel = Util.connect(server);

        Util.sendFromClient(channel, new TextWebSocketFrame("hello"));

        // Assert the message was still handled and the connection was not closed
        assertThat(messages).as("Message handler should still be called").containsExactly("hello");
        assertThat(channel.isOpen()).as("Channel should stay open").isTrue();
    }
}
