package pl.mbaracz.jwebsockets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpRequestEncoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class HandshakeTimeoutTest {

    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>("/chat")
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            );
    }

    /**
     * Creates a channel with the server's pipeline and stops its clock, so the test decides when the timeout expires.
     */
    private EmbeddedChannel newChannel() {
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));
        channel.freezeTime();
        return channel;
    }

    /**
     * Encodes an upgrade request the way a client sends it.
     */
    private static ByteBuf encodeRequest() {
        EmbeddedChannel encoder = Util.newEmbeddedChannel(new HttpRequestEncoder());
        encoder.writeOutbound(Util.createHttpRequest("/chat"));

        ByteBuf request = Unpooled.buffer();
        ByteBuf buffer;

        while ((buffer = encoder.readOutbound()) != null) {
            request.writeBytes(buffer);
            buffer.release();
        }

        return request;
    }

    private static void advanceTime(EmbeddedChannel channel, long seconds) {
        channel.advanceTimeBy(seconds, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
    }

    @Test
    void shouldCloseConnectionWhenNoRequestArrivesWithinDefaultTimeout() {
        EmbeddedChannel channel = newChannel();

        advanceTime(channel, 9);
        assertThat(channel.isOpen()).as("Connection should stay open before the timeout").isTrue();

        advanceTime(channel, 1);
        assertThat(channel.isOpen()).as("Connection should be closed after the default 10 seconds").isFalse();
    }

    @Test
    void shouldCloseConnectionWhenConfiguredTimeoutExpires() {
        server.configure(configurer -> configurer.setHandshakeTimeout(Duration.ofSeconds(2)));
        EmbeddedChannel channel = newChannel();

        advanceTime(channel, 2);

        assertThat(channel.isOpen()).as("Connection should be closed after the configured timeout").isFalse();
    }

    @Test
    void shouldCloseConnectionWhenRequestArrivesTooSlowly() {
        EmbeddedChannel channel = newChannel();
        ByteBuf request = encodeRequest();

        // Each part arrives well within the timeout, but the request is not complete before it expires
        for (int second = 0; second < 10; second++) {
            channel.writeInbound(request.readRetainedSlice(1));
            advanceTime(channel, 1);
        }

        request.release();

        assertThat(channel.isOpen()).as("Connection should be closed after the timeout").isFalse();
        assertThat(server.getConnectedSessions()).as("No session should be registered").isEmpty();
    }

    @Test
    void shouldKeepConnectionOpenWhenHandshakeCompletesInTime() {
        EmbeddedChannel channel = newChannel();

        channel.writeInbound(encodeRequest());
        channel.releaseOutbound();
        advanceTime(channel, 60);

        assertThat(channel.isOpen()).as("Connection should stay open").isTrue();
        assertThat(server.getConnectedSessions()).as("Session should be registered").hasSize(1);
    }
}
