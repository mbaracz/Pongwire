package pl.mbaracz.jwebsockets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import pl.mbaracz.jwebsockets.handler.UpgradeResult;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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

        PipelineResult result = sendRequestThroughPipeline(request);

        assertThat(result.response().status()).as("Should reject the body").isEqualTo(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
        assertThat(opened).as("Session should not be opened").isEmpty();
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

    @Test
    void shouldSelectNoSubprotocolWhenNoRequestedOneIsSupported() {
        server.configure(configurer -> configurer.setSubprotocols("superchat"));

        FullHttpRequest request = Util.createHttpRequest("/chat");
        request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, "chat");

        HttpResponse response = sendRequest(request);

        // Assert the connection was upgraded without a subprotocol
        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
        assertThat(response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL)).as("Should not select a subprotocol").isNull();
        assertThat(opened.getFirst().getSubprotocol()).as("Session should have no subprotocol").isNull();
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
    void shouldIgnoreFrameWhenHandshakeHasNotCompleted() {
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerHandler<>(server));

        channel.writeInbound(new TextWebSocketFrame("hello"));

        assertThat(channel.isOpen()).as("Connection should stay open").isTrue();
        assertThat(channel.<Object>readOutbound()).as("Should not respond").isNull();
    }
}
