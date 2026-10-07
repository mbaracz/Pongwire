package pl.mbaracz.jwebsockets;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionTest {

    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                    .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                    .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            );
    }

    @Nested
    class PathTests {
        @Test
        void shouldUpgradeConnectionWhenPathIsValid() {
            WebSocketServerHandler<String, Object> handler = new WebSocketServerHandler<>(server);
            EmbeddedChannel channel = Util.newEmbeddedChannel(handler);
            channel.pipeline().addFirst(new HttpServerCodec());

            // Construct http request
            String path = "/";
            FullHttpRequest request = Util.createHttpRequest(path);

            // Send request
            channel.writeInbound(request);

            // Assert connection was upgraded
            Object outboundMessage = channel.readOutbound();
            assertThat(outboundMessage).isInstanceOf(ByteBuf.class);
            ByteBuf buffer = (ByteBuf) outboundMessage;
            String responseContent = buffer.toString(CharsetUtil.UTF_8);
            assertThat(responseContent).contains("101 Switching Protocols");
            buffer.release();
        }

        @Test
        void shouldCloseConnectionWhenPathIsInvalid() {
            EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));

            // Construct http request
            String path = "/foo";
            FullHttpRequest request = Util.createHttpRequest(path);

            // Send request
            channel.writeInbound(request);

            // Read response
            FullHttpResponse response = channel.readOutbound();

            // Assert expected behaviour
            assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
            assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
            assertThat(channel.isActive()).as("Channel should not be active").isFalse();
            response.release();
        }
    }

    @Nested
    class CustomPathTests {

        private WebSocketServer<String, Object> customPathServer;

        @BeforeEach
        void setUp() {
            customPathServer = new WebSocketServer<String, Object>("/foo")
                .configure(configurer -> configurer
                        .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                        .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                );
        }

        @Test
        void shouldOpenConnectionWhenCustomPathIsValid() {
            WebSocketServerHandler<String, Object> handler = new WebSocketServerHandler<>(customPathServer);
            EmbeddedChannel channel = Util.newEmbeddedChannel(handler);
            channel.pipeline().addFirst(new HttpServerCodec());

            // Construct http request
            String path = "/foo";
            FullHttpRequest request = Util.createHttpRequest(path);

            // Send request
            channel.writeInbound(request);

            // Assert expected behaviour
            assertThat(channel.isOpen()).as("Channel should be opened").isTrue();
            assertThat(channel.isActive()).as("Channel should be active").isTrue();
        }

        @Test
        void shouldCloseConnectionWhenCustomPathIsInvalid() {
            EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(customPathServer));

            // Construct http request
            String path = "/";
            FullHttpRequest request = Util.createHttpRequest(path);

            // Send request
            channel.writeInbound(request);

            // Read response
            FullHttpResponse response = channel.readOutbound();

            // Assert expected behaviour
            assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
            assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
            assertThat(channel.isActive()).as("Channel should not be active").isFalse();
            response.release();
        }
    }

    @Nested
    class OriginTests {
        @Test
        void shouldOpenConnectionWhenOriginIsValidViaPattern() {
            server.configure(configurer -> configurer.setAllowedOriginPattern(Pattern.compile("^(http|https)://example\\.com$")));

            EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
            channel.pipeline().addFirst(new HttpServerCodec());

            // Construct http request
            String path = "/";
            FullHttpRequest request = Util.createHttpRequest(path);

            // Add origin header
            request.headers().add(HttpHeaderNames.ORIGIN, "http://example.com");

            // Send request
            channel.writeInbound(request);

            // Assert expected behavior
            assertThat(channel.isOpen()).as("Channel should be open").isTrue();
            assertThat(channel.isActive()).as("Channel should be active").isTrue();
        }

        @Test
        void shouldForbidConnectionWhenOriginIsInvalidViaPattern() {
            server.configure(configurer -> configurer.setAllowedOriginPattern(Pattern.compile("^(http|https)://example\\.com$")));

            EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));

            // Construct http request
            String path = "/";
            FullHttpRequest request = Util.createHttpRequest(path);

            // Add origin header
            request.headers().add(HttpHeaderNames.ORIGIN, "http://wrong.com");

            // Send request
            channel.writeInbound(request);

            // Assert expected behavior
            FullHttpResponse response = channel.readOutbound();
            assertThat(response.status()).as("Should receive forbidden response").isEqualTo(HttpResponseStatus.FORBIDDEN);
            assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
            assertThat(channel.isActive()).as("Channel should not be active").isFalse();
            response.release();
        }

        @Test
        void shouldOpenConnectionWhenOriginIsValid() {
            server.configure(configurer -> configurer.setAllowedOrigins("http://example.com"));

            EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));
            channel.pipeline().addFirst(new HttpServerCodec());

            // Construct http request
            String path = "/";
            FullHttpRequest request = Util.createHttpRequest(path);

            // Add origin header
            request.headers().add(HttpHeaderNames.ORIGIN, "http://example.com");

            // Send request
            channel.writeInbound(request);

            // Assert expected behavior
            assertThat(channel.isOpen()).as("Channel should be open").isTrue();
            assertThat(channel.isActive()).as("Channel should be active").isTrue();
        }

        @Test
        void shouldForbidConnectionWhenOriginIsInvalid() {
            server.configure(configurer -> configurer.setAllowedOrigins("http://example.com"));

            EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));

            // Construct http request
            String path = "/";
            FullHttpRequest request = Util.createHttpRequest(path);

            // Add origin header
            request.headers().add(HttpHeaderNames.ORIGIN, "http://wrong.com");

            // Send request
            channel.writeInbound(request);

            // Assert expected behavior
            FullHttpResponse response = channel.readOutbound();
            assertThat(response.status()).as("Should receive forbidden response").isEqualTo(HttpResponseStatus.FORBIDDEN);
            assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
            assertThat(channel.isActive()).as("Channel should not be active").isFalse();
            response.release();
        }

        @Test
        void shouldForbidConnectionWhenOriginIsNotProvided() {
            server.configure(configurer -> configurer.setAllowedOrigins("http://example.com"));

            EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));

            // Construct http request
            String path = "/";
            FullHttpRequest request = Util.createHttpRequest(path);

            // Send request
            channel.writeInbound(request);

            // Assert expected behavior
            FullHttpResponse response = channel.readOutbound();
            assertThat(response.status()).as("Should receive forbidden response").isEqualTo(HttpResponseStatus.FORBIDDEN);
            assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
            assertThat(channel.isActive()).as("Channel should not be active").isFalse();
            response.release();
        }
    }
}
