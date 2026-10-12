package pl.mbaracz.pongwire;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class OpenHandlerTest {

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
    void shouldCallOpenHandlerWhenHandshakeIsDone() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        server.onOpen((session) -> latch.countDown());

        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
        channel.pipeline().addFirst(new HttpServerCodec());
        Util.performHandshake(channel, "/");

        assertThat(latch.await(5, TimeUnit.SECONDS)).as("Open handler was not called").isTrue();
    }
}
