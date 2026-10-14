package pl.mbaracz.pongwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocket13FrameDecoder;
import io.netty.handler.codec.http.websocketx.WebSocket13FrameEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import pl.mbaracz.pongwire.handler.UpgradeResult;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HandshakeTest {

    private final List<WebSocketSession<String, Object>> opened = new ArrayList<>();
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>("/chat")
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            )
            .onOpen(opened::add);
    }

    /**
     * Sends the request through an HTTP codec and decodes the response the way a client does.
     */
    private HttpResponse sendRequest(FullHttpRequest request) {
        EmbeddedChannel channel = Util.newEmbeddedChannel(new HttpServerCodec(), new WebSocketServerHandler<>(server));
        channel.writeInbound(request);

        EmbeddedChannel client = Util.newEmbeddedChannel(new HttpResponseDecoder());
        ByteBuf buffer;

        while ((buffer = channel.readOutbound()) != null) {
            client.writeInbound(buffer);
        }

        return client.readInbound();
    }

    /**
     * Encodes a request and sends it through the complete server pipeline, including the HTTP aggregator.
     */
    private PipelineResult sendRequestThroughPipeline(FullHttpRequest request) {
        EmbeddedChannel requestEncoder = Util.newEmbeddedChannel(new HttpRequestEncoder());
        requestEncoder.writeOutbound(request);

        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));
        ByteBuf buffer;

        while ((buffer = requestEncoder.readOutbound()) != null) {
            channel.writeInbound(buffer);
        }

        EmbeddedChannel responseDecoder = Util.newEmbeddedChannel(new HttpResponseDecoder());
        while ((buffer = channel.readOutbound()) != null) {
            responseDecoder.writeInbound(buffer);
        }

        return new PipelineResult(responseDecoder.readInbound(), channel.isOpen());
    }

    private FullHttpRequest createRequestWithHeader(int size) {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/chat");
        request.headers().set(Util.getDefaultHeaders());
        request.headers().set("X-Large", "x".repeat(size));
        return request;
    }

    /**
     * Creates a pre-RFC 6455 (Hixie-75) request, which has no Sec-WebSocket-Version header, with the given body.
     */
    private FullHttpRequest createLegacyRequest(ByteBuf content) {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/chat", content);
        request.headers()
            .set(HttpHeaderNames.HOST, "localhost:8080")
            .set(HttpHeaderNames.UPGRADE, "WebSocket")
            .set(HttpHeaderNames.CONNECTION, "Upgrade")
            .set(HttpHeaderNames.ORIGIN, "http://localhost:8080");
        return request;
    }

    private record PipelineResult(HttpResponse response, boolean channelOpen) {
    }

    @Test
    void shouldUpgradeConnectionWhenUpgradeHeaderHasDifferentCase() {
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.UPGRADE, "WebSocket");

        HttpResponse response = sendRequest(request);

        // Assert upgrade token is compared case-insensitively
        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
    }

    @Test
    void shouldUpgradeConnectionWhenHandshakeHeadersAreWithinMaximumSize() {
        server.configure(configurer -> configurer.setMaxHandshakeHeaderSize(1024));

        PipelineResult result = sendRequestThroughPipeline(createRequestWithHeader(128));

        assertThat(result.response().status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
        assertThat(result.channelOpen()).as("Connection should stay open").isTrue();
    }

    @Test
    void shouldRejectHandshakeWhenHeadersExceedMaximumSize() {
        server.configure(configurer -> configurer.setMaxHandshakeHeaderSize(1024));

        PipelineResult result = sendRequestThroughPipeline(createRequestWithHeader(2048));

        assertThat(result.response().status()).as("Should reject request with oversized headers").isEqualTo(HttpResponseStatus.BAD_REQUEST);
        assertThat(result.channelOpen()).as("Connection should be closed").isFalse();
        assertThat(opened).as("Session should not be opened").isEmpty();
    }

    @Test
    void shouldRespondWithBadRequestWhenUpgradeHeaderDoesNotContainWebSocket() {
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.UPGRADE, "h2c");

        HttpResponse response = sendRequest(request);

        // Assert request is rejected
        assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
    }

    @Test
    void shouldUpgradeConnectionWhenConnectionHeaderHasMultipleTokens() {
        // Firefox sends the upgrade token together with keep-alive
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.CONNECTION, "keep-alive, Upgrade");

        HttpResponse response = sendRequest(request);

        // Assert upgrade token is found in the token list
        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
    }

    @Test
    void shouldRespondWithBadRequestWhenConnectionHeaderHasNoUpgradeToken() {
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.CONNECTION, "keep-alive");

        HttpResponse response = sendRequest(request);

        // Assert request is rejected before the handshake
        assertThat(response).as("Should receive a response").isNotNull();
        assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
    }

    @Test
    void shouldUpgradeConnectionWhenPathHasQueryString() {
        FullHttpRequest request = Util.createHttpRequest("/chat?token=abc");

        HttpResponse response = sendRequest(request);

        // Assert endpoint is matched by path only
        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
    }

    @Test
    void shouldRespondWithBadRequestWhenPathWithQueryStringDoesNotMatch() {
        FullHttpRequest request = Util.createHttpRequest("/other?path=/chat");

        HttpResponse response = sendRequest(request);

        // Assert query string does not affect matching
        assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
    }

    @Test
    void shouldRespondWithBadRequestWhenRequestUriIsMalformed() {
        FullHttpRequest request = Util.createHttpRequest("/chat?filter={name}");

        HttpResponse response = sendRequest(request);

        // Assert malformed URI is rejected instead of failing the handler
        assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
    }

    @ParameterizedTest
    @ValueSource(strings = {"ws://localhost/chat", "//evil.example/chat", "*"})
    void shouldRespondWithBadRequestWhenRequestTargetIsNotInOriginForm(String target) {
        HttpResponse response = sendRequest(Util.createHttpRequest(target));

        assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
        assertThat(opened).as("Session should not be opened").isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://localhost:8081/chat?token=1", "HTTPS://localhost:8081/chat?token=1"})
    void shouldUpgradeConnectionWhenRequestTargetIsAbsolute(String target) {
        List<UpgradeRequest> upgrades = new ArrayList<>();
        server.onUpgrade((upgradeRequest, _) -> {
            upgrades.add(upgradeRequest);
            return UpgradeResult.accept(null);
        });

        HttpResponse response = sendRequest(Util.createHttpRequest(target));

        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
        assertThat(upgrades).singleElement().satisfies(upgrade -> {
            assertThat(upgrade.getPath()).as("Path of the absolute target").isEqualTo("/chat");
            assertThat(upgrade.getQueryParameters()).as("Query of the absolute target").containsEntry("token", List.of("1"));
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"/chat#ignored", "/chat?token=abc#ignored", "/chat#", "http://localhost:8081/chat#ignored"})
    void shouldRespondWithBadRequestWhenRequestTargetHasFragment(String target) {
        HttpResponse response = sendRequest(Util.createHttpRequest(target));

        assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
        assertThat(opened).as("Session should not be opened").isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://localhost:8081", "localhost:8081/chat", "local host", ""})
    void shouldRespondWithBadRequestWhenHostHeaderIsMalformed(String host) {
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.HOST, host);

        HttpResponse response = sendRequest(request);

        assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
    }

    @Test
    void shouldRespondWithBadRequestWhenHostHeaderIsRepeated() {
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().add(HttpHeaderNames.HOST, "other.example");

        HttpResponse response = sendRequest(request);

        assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
    }

    @ParameterizedTest
    @ValueSource(strings = {"[::1]:8081", "127.0.0.1", "chat.example.com:"})
    void shouldUpgradeConnectionWhenHostHeaderIsAddressOrHasNoPort(String host) {
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.HOST, host);

        HttpResponse response = sendRequest(request);

        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
    }

    @Test
    void shouldRespondWithBadRequestWhenRequestIsHttp10() {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/chat");
        request.headers().set(Util.getDefaultHeaders());

        HttpResponse response = sendRequest(request);

        assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
    }

    @Test
    void shouldRespondWithBadRequestWhenHostHeaderIsMissing() {
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().remove(HttpHeaderNames.HOST);

        HttpResponse response = sendRequest(request);

        assertThat(response.status()).as("Should receive bad request response").isEqualTo(HttpResponseStatus.BAD_REQUEST);
    }

    @Test
    void shouldRespondWithUpgradeRequiredAndCloseWhenDraftVersionIsRequested() {
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_VERSION, "8");

        PipelineResult result = sendRequestThroughPipeline(request);

        assertThat(result.response().status()).as("Should require RFC 6455").isEqualTo(HttpResponseStatus.UPGRADE_REQUIRED);
        assertThat(result.response().headers().get(HttpHeaderNames.SEC_WEBSOCKET_VERSION)).as("Supported version").isEqualTo("13");
        assertThat(result.channelOpen()).as("Connection should be closed").isFalse();
        assertThat(opened).as("Session should not be opened").isEmpty();
    }

    @Test
    void shouldUpgradeConnectionWhenRequestLineIsLongerThanNettyDefaultButWithinMaximumSize() {
        server.configure(configurer -> configurer.setMaxHandshakeHeaderSize(16 * 1024));

        PipelineResult result = sendRequestThroughPipeline(Util.createHttpRequest("/chat?token=" + "x".repeat(8 * 1024)));

        assertThat(result.response().status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
    }

    @Test
    void shouldRejectHandshakeWhenBodyExceedsMaximumSize() {
        server.configure(configurer -> configurer.setMaxHandshakeHeaderSize(1024));
        FullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, "/chat", Unpooled.wrappedBuffer(new byte[2048])
        );
        request.headers().set(Util.getDefaultHeaders()).setInt(HttpHeaderNames.CONTENT_LENGTH, 2048);

        List<Integer> rejections = observeRejections();

        PipelineResult result = sendRequestThroughPipeline(request);

        assertThat(result.response().status()).as("Should reject the body").isEqualTo(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
        assertThat(rejections).as("Observer should see the rejection once").containsExactly(413);
        assertThat(opened).as("Session should not be opened").isEmpty();
    }

    @Test
    void shouldReportRejectedBodyToObserverWhenClientExpectsContinue() {
        server.configure(configurer -> configurer.setMaxHandshakeHeaderSize(1024));
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/chat");
        request.headers().set(Util.getDefaultHeaders())
            .set(HttpHeaderNames.EXPECT, HttpHeaderValues.CONTINUE)
            .setInt(HttpHeaderNames.CONTENT_LENGTH, 2048);
        List<Integer> rejections = observeRejections();

        PipelineResult result = sendRequestThroughPipeline(request);

        assertThat(result.response().status()).as("Should reject the announced body").isEqualTo(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
        assertThat(rejections).as("Observer should see the rejection once").containsExactly(413);
    }

    private List<Integer> observeRejections() {
        List<Integer> rejections = new ArrayList<>();
        server.observer(new WebSocketServerObserver<>() {
            @Override
            public void handshakeRejected(int status) {
                rejections.add(status);
            }
        });
        return rejections;
    }

    @Test
    void shouldRespondWithUpgradeRequiredAndCloseWhenHixie75ClientConnects() {
        PipelineResult result = sendRequestThroughPipeline(createLegacyRequest(Unpooled.EMPTY_BUFFER));

        assertThat(result.response().status()).as("Should require RFC 6455").isEqualTo(HttpResponseStatus.UPGRADE_REQUIRED);
        assertThat(result.channelOpen()).as("Connection should be closed").isFalse();
        assertThat(opened).as("Session should not be opened").isEmpty();
    }

    @Test
    void shouldRespondWithUpgradeRequiredAndCloseWhenHixie76ClientConnects() {
        FullHttpRequest request = createLegacyRequest(Unpooled.wrappedBuffer(new byte[8]));
        request.headers()
            .set(HttpHeaderNames.SEC_WEBSOCKET_KEY1, "1 1")
            .set(HttpHeaderNames.SEC_WEBSOCKET_KEY2, "1 1");

        PipelineResult result = sendRequestThroughPipeline(request);

        assertThat(result.response().status()).as("Should require RFC 6455").isEqualTo(HttpResponseStatus.UPGRADE_REQUIRED);
        assertThat(result.channelOpen()).as("Connection should be closed").isFalse();
        assertThat(opened).as("Session should not be opened").isEmpty();
    }

    @Test
    void shouldSelectSubprotocolWhenClientRequestsSupportedOne() {
        server.configure(configurer -> configurer.setSubprotocols("superchat"));

        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, "chat, superchat");

        HttpResponse response = sendRequest(request);

        // Assert the subprotocol supported by both sides was selected
        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
        assertThat(response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL))
            .as("Should select the supported subprotocol")
            .isEqualTo("superchat");
        assertThat(opened.getFirst().getSubprotocol()).as("Session should have the selected subprotocol").isEqualTo("superchat");
    }

    @ParameterizedTest
    @ValueSource(strings = {"chat", "chat, superchat2", ""})
    void shouldRespondWithBadRequestWhenNoRequestedSubprotocolIsSupported(String requested) {
        server.configure(configurer -> configurer.setSubprotocols("superchat"));

        FullHttpRequest request = Util.createHttpRequest("/chat");
        if (!requested.isEmpty()) {
            request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, requested);
        }

        HttpResponse response = sendRequest(request);

        assertThat(response.status()).as("Should reject the upgrade").isEqualTo(HttpResponseStatus.BAD_REQUEST);
        assertThat(opened).as("Session should not be opened").isEmpty();
    }

    @Test
    void shouldSelectSubprotocolWhenItIsRequestedInLaterHeaderLine() {
        server.configure(configurer -> configurer.setSubprotocols("superchat"));

        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers()
            .add(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, "chat")
            .add(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, "superchat");

        HttpResponse response = sendRequest(request);

        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
        assertThat(opened.getFirst().getSubprotocol()).as("Selected subprotocol").isEqualTo("superchat");
    }

    @Test
    void shouldRejectWildcardSubprotocol() {
        assertThatThrownBy(() -> server.configure(configurer -> configurer.setSubprotocols("chat", "*")))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldSelectNoSubprotocolWhenSubprotocolsAreNotConfigured() {
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, "chat");

        HttpResponse response = sendRequest(request);

        // Assert the connection was upgraded without a subprotocol, as before subprotocol support
        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
        assertThat(response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL)).as("Should not select a subprotocol").isNull();
        assertThat(opened.getFirst().getSubprotocol()).as("Session should have no subprotocol").isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "abc", "AAAAAAAAAAAAAAAAAAAA"})
    void shouldRespondWithBadRequestAndCloseWhenKeyIsInvalid(String key) {
        List<UpgradeRequest> upgrades = new ArrayList<>();
        server.onUpgrade((upgradeRequest, _) -> {
            upgrades.add(upgradeRequest);
            return UpgradeResult.accept(null);
        });

        // "AAAAAAAAAAAAAAAAAAAA" is valid Base64, but decodes to 15 bytes instead of 16
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_KEY, key);

        PipelineResult result = sendRequestThroughPipeline(request);

        assertThat(result.response().status()).as("Should reject the handshake").isEqualTo(HttpResponseStatus.BAD_REQUEST);
        assertThat(result.channelOpen()).as("Connection should be closed").isFalse();
        assertThat(upgrades).as("Upgrade handler should not be invoked").isEmpty();
        assertThat(opened).as("Session should not be opened").isEmpty();
    }

    @Test
    void shouldRespondWithBadRequestAndCloseWhenKeyIsMissing() {
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().remove(HttpHeaderNames.SEC_WEBSOCKET_KEY);

        PipelineResult result = sendRequestThroughPipeline(request);

        assertThat(result.response().status()).as("Should reject the handshake").isEqualTo(HttpResponseStatus.BAD_REQUEST);
        assertThat(result.channelOpen()).as("Connection should be closed").isFalse();
        assertThat(opened).as("Session should not be opened").isEmpty();
    }

    @Test
    void shouldRespondWithBadRequestAndCloseWhenNettyRejectsHandshake() {
        // The token list passes the server's own check, while Netty requires the value to be exactly "websocket"
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.UPGRADE, "websocket, h2c");

        PipelineResult result = sendRequestThroughPipeline(request);

        assertThat(result.response().status()).as("Should reject the handshake").isEqualTo(HttpResponseStatus.BAD_REQUEST);
        assertThat(result.channelOpen()).as("Connection should be closed").isFalse();
        assertThat(opened).as("Session should not be opened").isEmpty();
    }

    @Test
    void shouldRespondWithUpgradeRequiredAndCloseWhenVersionIsUnsupported() {
        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_VERSION, "99");

        PipelineResult result = sendRequestThroughPipeline(request);

        assertThat(result.response().status()).as("Should require another version").isEqualTo(HttpResponseStatus.UPGRADE_REQUIRED);
        assertThat(result.response().headers().get(HttpHeaderNames.SEC_WEBSOCKET_VERSION))
            .as("Should name the supported version")
            .isEqualTo("13");
        assertThat(result.channelOpen()).as("Connection should be closed").isFalse();
    }

    @Test
    void shouldRespondWithBadRequestAndCloseWhenMethodIsNotGet() {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/chat");
        request.headers().set(Util.getDefaultHeaders());

        PipelineResult result = sendRequestThroughPipeline(request);

        assertThat(result.response().status()).as("Should reject the handshake").isEqualTo(HttpResponseStatus.BAD_REQUEST);
        assertThat(result.channelOpen()).as("Connection should be closed").isFalse();
    }

    @Test
    void shouldHandleFramesSentInSameReadAsUpgradeRequestAfterSessionOpens() {
        List<String> events = new ArrayList<>();
        server
            .onOpen(_ -> events.add("open"))
            .onMessage((_, message) -> events.add(message));
        EmbeddedChannel requestEncoder = Util.newEmbeddedChannel(new HttpRequestEncoder());
        requestEncoder.writeOutbound(Util.createHttpRequest("/chat"));
        EmbeddedChannel frameEncoder = Util.newEmbeddedChannel(new WebSocket13FrameEncoder(true));
        frameEncoder.writeOutbound(new TextWebSocketFrame("first"), new TextWebSocketFrame("second"));
        CompositeByteBuf read = Unpooled.compositeBuffer();
        ByteBuf buffer;

        while ((buffer = requestEncoder.readOutbound()) != null) {
            read.addComponent(true, buffer);
        }
        while ((buffer = frameEncoder.readOutbound()) != null) {
            read.addComponent(true, buffer);
        }

        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));
        channel.writeInbound(read);
        channel.releaseOutbound();

        assertThat(events).as("Messages should follow the opening of the session").containsExactly("open", "first", "second");
    }

    @Test
    void shouldFlushPongForPingSentInSameReadAsUpgradeRequestWhenResponseIsWrittenLater() {
        EmbeddedChannel requestEncoder = Util.newEmbeddedChannel(new HttpRequestEncoder());
        requestEncoder.writeOutbound(Util.createHttpRequest("/chat"));
        EmbeddedChannel frameEncoder = Util.newEmbeddedChannel(new WebSocket13FrameEncoder(true));
        frameEncoder.writeOutbound(new PingWebSocketFrame(Unpooled.copiedBuffer("ping", StandardCharsets.UTF_8)));
        CompositeByteBuf read = Unpooled.compositeBuffer();
        ByteBuf buffer;

        while ((buffer = requestEncoder.readOutbound()) != null) {
            read.addComponent(true, buffer);
        }
        while ((buffer = frameEncoder.readOutbound()) != null) {
            read.addComponent(true, buffer);
        }

        // The 101 response leaves the server after the read that carried the Ping, like behind a full socket buffer
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));
        AtomicBoolean holding = new AtomicBoolean(true);
        channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void flush(ChannelHandlerContext context) {
                if (!holding.get()) {
                    context.flush();
                }
            }
        });
        channel.writeInbound(read);
        assertThat(opened).as("Session should open once the response is written").isEmpty();

        holding.set(false);
        channel.flush();

        assertThat(opened).as("Session should be opened").hasSize(1);
        assertThat(channel.isOpen()).as("Connection should stay open").isTrue();
        assertThat(channel.unsafe().outboundBuffer().totalPendingWriteBytes()).as("The Pong should be flushed").isZero();
        assertThat(readFramesAfterResponse(channel)).as("Written frames").singleElement().isInstanceOf(PongWebSocketFrame.class);
    }

    /**
     * Decodes the HTTP response the server wrote and then the WebSocket frames following it.
     */
    private static List<WebSocketFrame> readFramesAfterResponse(EmbeddedChannel channel) {
        EmbeddedChannel client = Util.newEmbeddedChannel(new HttpResponseDecoder());
        ByteBuf buffer;

        while ((buffer = channel.readOutbound()) != null) {
            client.writeInbound(buffer);
        }
        assertThat(client.<Object>readInbound()).as("Response").isInstanceOf(HttpResponse.class);

        // After the 101 response the decoder passes the remaining bytes through
        EmbeddedChannel frameDecoder = Util.newEmbeddedChannel(new WebSocket13FrameDecoder(false, true, 65536));
        Object remaining;

        while ((remaining = client.readInbound()) != null) {
            if (remaining instanceof ByteBuf bytes) {
                frameDecoder.writeInbound(bytes);
            } else {
                ReferenceCountUtil.release(remaining);
            }
        }

        List<WebSocketFrame> frames = new ArrayList<>();
        WebSocketFrame frame;

        while ((frame = frameDecoder.readInbound()) != null) {
            frames.add(frame);
            frame.release();
        }
        return frames;
    }

    @Test
    void shouldIgnoreFrameWhenHandshakeHasNotCompleted() {
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));

        channel.writeInbound(new TextWebSocketFrame("hello"));

        assertThat(channel.isOpen()).as("Connection should stay open").isTrue();
        assertThat(channel.<Object>readOutbound()).as("Should not respond").isNull();
    }
}
