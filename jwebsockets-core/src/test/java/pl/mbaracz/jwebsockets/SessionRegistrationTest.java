package pl.mbaracz.jwebsockets;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.jwebsockets.handler.UpgradeResult;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class SessionRegistrationTest {

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
    void shouldNotRegisterSessionBeforeHandshakeWhenConnectionIsOpened() {
        // Open connection without sending upgrade request
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));

        // Assert no session was registered
        assertThat(server.getSessionByChannelId(channel.id())).as("Session should not be registered").isNull();
        assertThat(server.getConnectedSessions()).as("Connected sessions should be empty").isEmpty();
    }

    /**
     * Creates a channel that records whether a session was registered when the HTTP response was written,
     * because rejected connections are closed (and unregistered) right after the response.
     */
    private EmbeddedChannel createChannel(AtomicBoolean registeredOnResponse) {
        return Util.newEmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
                registeredOnResponse.set(server.getSessionByChannelId(context.channel().id()) != null);
                context.write(message, promise);
            }
        }, new WebSocketServerHandler<>(server));
    }

    @Test
    void shouldNotRegisterSessionWhenPlainHttpRequestIsSent() {
        AtomicBoolean registeredOnResponse = new AtomicBoolean();
        EmbeddedChannel channel = createChannel(registeredOnResponse);

        // Send http request without upgrade headers
        channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));

        // Assert request was rejected and no session was registered
        FullHttpResponse response = channel.readOutbound();
        assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
        assertThat(registeredOnResponse.get()).as("Session should not be registered while request is rejected").isFalse();
        assertThat(server.getSessionByChannelId(channel.id())).as("Session should not be registered").isNull();
        response.release();
    }

    @Test
    void shouldNotRegisterSessionWhenUpgradeIsRejected() {
        AtomicBoolean registeredOnResponse = new AtomicBoolean();
        server.onUpgrade((_, _) -> UpgradeResult.reject());
        EmbeddedChannel channel = createChannel(registeredOnResponse);

        // Send upgrade request
        Util.performHandshake(channel, "/");

        // Assert upgrade was rejected and no session was registered
        FullHttpResponse response = channel.readOutbound();
        assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
        assertThat(registeredOnResponse.get()).as("Session should not be registered while upgrade is rejected").isFalse();
        assertThat(server.getSessionByChannelId(channel.id())).as("Session should not be registered").isNull();
        response.release();
    }

    @Test
    void shouldCloseWithoutRegisteringSessionWhenHandshakeCannotComplete() {
        // Without an HTTP codec in the pipeline the handshake fails
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.performHandshake(channel, "/");

        // Assert no session was registered
        assertThat(server.getSessionByChannelId(channel.id())).as("Session should not be registered").isNull();
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
    }

    @Test
    void shouldRegisterSessionBeforeOpenHandlerWhenHandshakeIsCompleted() {
        AtomicBoolean registeredOnOpen = new AtomicBoolean();

        server.onOpen(session -> registeredOnOpen.set(server.getConnectedSessions().contains(session)));

        // Construct channel and perform handshake
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        Util.completeHandshake(channel, "/");

        // Assert session was registered before open handler was called
        assertThat(server.getSessionByChannelId(channel.id())).as("Session should be registered").isNotNull();
        assertThat(registeredOnOpen.get()).as("Session should be registered when open handler is called").isTrue();
    }
}
