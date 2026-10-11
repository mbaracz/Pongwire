package pl.mbaracz.jwebsockets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class CompressionTest {

    // Marks a compressed message (RFC 7692, section 6)
    private static final int RSV1 = 0b100;

    // Empty block that ends a compressed message, the sender removes it (RFC 7692, section 7.2.1)
    private static final byte[] MESSAGE_TAIL = {0x00, 0x00, (byte) 0xff, (byte) 0xff};

    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                .setCompressionEnabled(true)
            );
    }

    /**
     * Sends the handshake request with the extension offer and decodes the response the way a client does.
     */
    private static HttpResponse handshake(EmbeddedChannel channel, String extensionOffer) {
        FullHttpRequest request = Util.createHttpRequest("/");
        request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS, extensionOffer);
        channel.writeInbound(request);

        EmbeddedChannel client = Util.newEmbeddedChannel(new HttpResponseDecoder());
        ByteBuf buffer;

        while ((buffer = channel.readOutbound()) != null) {
            client.writeInbound(buffer);
        }

        return client.readInbound();
    }

    /**
     * Creates a channel with the server's pipeline and completes a handshake that negotiates compression.
     */
    private static EmbeddedChannel connectWithCompression(WebSocketServer<String, Object> server) {
        return connectWithExtensions(server, "permessage-deflate");
    }

    /**
     * Offers permessage-deflate and checks the extensions the server answered with.
     */
    private static EmbeddedChannel connectWithExtensions(WebSocketServer<String, Object> server, String expectedExtensions) {
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));
        HttpResponse response = handshake(channel, "permessage-deflate");

        assertThat(response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS))
            .as("Should negotiate compression")
            .isEqualTo(expectedExtensions);
        return channel;
    }

    /**
     * Compresses the text the way a client compresses a message (RFC 7692, section 7.2.1).
     */
    private static ByteBuf compress(String text) {
        Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        deflater.setInput(text.getBytes(StandardCharsets.UTF_8));

        byte[] buffer = new byte[1024];
        int length = deflater.deflate(buffer, 0, buffer.length, Deflater.SYNC_FLUSH);
        deflater.end();

        // The sync flush ends the data with the empty block, which is not sent
        return Unpooled.wrappedBuffer(buffer, 0, length - MESSAGE_TAIL.length);
    }

    /**
     * Decompresses a message the way a client does (RFC 7692, section 7.2.2).
     */
    private static String decompress(ByteBuf content) throws DataFormatException {
        byte[] compressed = ByteBufUtil.getBytes(content);

        // Restore the empty block the sender removed
        byte[] input = Arrays.copyOf(compressed, compressed.length + MESSAGE_TAIL.length);
        System.arraycopy(MESSAGE_TAIL, 0, input, compressed.length, MESSAGE_TAIL.length);

        Inflater inflater = new Inflater(true);
        inflater.setInput(input);

        byte[] buffer = new byte[1024];
        int length = inflater.inflate(buffer);
        inflater.end();

        return new String(buffer, 0, length, StandardCharsets.UTF_8);
    }

    @Test
    void shouldNotNegotiateCompressionWhenItIsNotEnabled() {
        server.configure(configurer -> configurer.setCompressionEnabled(false));

        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));

        HttpResponse response = handshake(channel, "permessage-deflate; client_max_window_bits");

        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
        assertThat(response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS)).as("Should not negotiate any extension").isNull();
    }

    @Test
    void shouldNegotiatePermessageDeflateWhenClientOffersIt() {
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));

        HttpResponse response = handshake(channel, "permessage-deflate; client_max_window_bits");

        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
        assertThat(response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS))
            .as("Should negotiate permessage-deflate")
            .isEqualTo("permessage-deflate");
    }

    @Test
    void shouldAcceptServerNoContextTakeoverWhenClientRequestsIt() {
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));

        HttpResponse response = handshake(channel, "permessage-deflate; server_no_context_takeover");

        assertThat(response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS))
            .as("Should accept server_no_context_takeover")
            .isEqualTo("permessage-deflate;server_no_context_takeover");
    }

    @Test
    void shouldResetServerContextWhenItIsConfiguredAlthoughClientDidNotAsk() {
        server.configure(configurer -> configurer.setServerNoContextTakeover(true));

        connectWithExtensions(server, "permessage-deflate;server_no_context_takeover");
    }

    @Test
    void shouldSendFramesBelowThresholdUncompressed() throws DataFormatException {
        List<WebSocketSession<String, Object>> opened = new ArrayList<>();
        server
            .configure(configurer -> configurer.setCompressionThreshold(10))
            .onOpen(opened::add);
        EmbeddedChannel channel = connectWithCompression(server);

        opened.getFirst().sendMessage("short");
        WebSocketFrame small = assertThat(Util.readFromServer(channel)).asInstanceOf(type(TextWebSocketFrame.class)).actual();
        assertThat(small.rsv()).as("Small frame should not be compressed").isZero();
        small.release();

        opened.getFirst().sendMessage("long enough to compress");
        WebSocketFrame large = assertThat(Util.readFromServer(channel)).asInstanceOf(type(TextWebSocketFrame.class)).actual();
        assertThat(large.rsv()).as("Large frame should be compressed").isEqualTo(RSV1);
        assertThat(decompress(large.content())).as("Decompressed message").isEqualTo("long enough to compress");
        large.release();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 10})
    void shouldRejectCompressionLevelOutsideZlibRange(int level) {
        assertThatThrownBy(() -> server.configure(configurer -> configurer.setCompressionLevel(level)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldAcceptOfferWhenClientRequestsFullServerWindow() {
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));

        HttpResponse response = handshake(channel, "permessage-deflate; server_max_window_bits=15");

        assertThat(response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS))
            .as("Should accept the offer")
            .startsWith("permessage-deflate");
    }

    @Test
    void shouldDeclineMalformedOfferAndUpgradeWithoutCompression() {
        List<WebSocketSession<String, Object>> opened = new ArrayList<>();
        server.onOpen(opened::add);
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));

        HttpResponse response = handshake(channel, "permessage-deflate; client_max_window_bits=abc");

        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
        assertThat(response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS)).as("Should decline the offer").isNull();
        assertThat(opened).as("Session should be opened").hasSize(1);
    }

    @Test
    void shouldNegotiateOfferFromLaterExtensionsHeader() {
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));
        FullHttpRequest request = Util.createHttpRequest("/");
        request.headers()
            .add(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS, "x-webkit-deflate-frame")
            .add(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS, "permessage-deflate");
        channel.writeInbound(request);

        EmbeddedChannel client = Util.newEmbeddedChannel(new HttpResponseDecoder());
        ByteBuf buffer;
        while ((buffer = channel.readOutbound()) != null) {
            client.writeInbound(buffer);
        }
        HttpResponse response = client.readInbound();

        assertThat(response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS)).as("Should negotiate compression").isEqualTo("permessage-deflate");
    }

    @Test
    void shouldCloseConnectionWithProtocolErrorWhenContinuationFrameHasRsv1() {
        List<String> received = new ArrayList<>();
        server.onMessage((_, message) -> received.add(message));
        EmbeddedChannel channel = connectWithCompression(server);
        ByteBuf compressed = compress("Hello, compressed world");
        int half = compressed.readableBytes() / 2;

        Util.sendFromClient(
            channel,
            new TextWebSocketFrame(false, RSV1, compressed.retainedSlice(0, half)),
            new ContinuationWebSocketFrame(true, RSV1, compressed.retainedSlice(half, compressed.readableBytes() - half))
        );
        compressed.release();

        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel))
            .as("Should send a close frame")
            .asInstanceOf(type(CloseWebSocketFrame.class))
            .actual();
        assertThat(closeFrame.statusCode()).as("Close status").isEqualTo(WebSocketCloseStatus.PROTOCOL_ERROR.code());
        closeFrame.release();
        assertThat(received).as("Message should not be handled").isEmpty();
    }

    @Test
    void shouldCloseConnectionWithProtocolErrorWhenDecompressionFailsWithCodecException() {
        EmbeddedChannel channel = connectWithCompression(server);

        // The deflate decoder fails like this when the first frame of a message inflates to nothing
        channel.pipeline().fireExceptionCaught(new DecoderException(new CodecException("cannot read uncompressed buffer")));

        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel))
            .as("Should send a close frame")
            .asInstanceOf(type(CloseWebSocketFrame.class))
            .actual();
        assertThat(closeFrame.statusCode()).as("Close status").isEqualTo(WebSocketCloseStatus.PROTOCOL_ERROR.code());
        closeFrame.release();
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
    }

    @Test
    void shouldDeclineOfferWhenClientRequestsServerWindowBelowFullSize() {
        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));

        HttpResponse response = handshake(channel, "permessage-deflate; server_max_window_bits=10");

        assertThat(response.status()).as("Should switch protocols").isEqualTo(HttpResponseStatus.SWITCHING_PROTOCOLS);
        assertThat(response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS)).as("Should decline the offer").isNull();
    }

    @Test
    void shouldPassDecompressedMessageToHandlerWhenCompressedMessageIsReceived() {
        List<String> received = new ArrayList<>();
        server.onMessage((_, message) -> received.add(message));

        EmbeddedChannel channel = connectWithCompression(server);

        Util.sendFromClient(channel, new TextWebSocketFrame(true, RSV1, compress("Hello")));

        assertThat(received).as("Should receive the decompressed message").isEqualTo(List.of("Hello"));
    }

    @Test
    void shouldCompressSentMessageWhenCompressionIsNegotiated() throws DataFormatException {
        List<WebSocketSession<String, Object>> opened = new ArrayList<>();
        server.onOpen(opened::add);

        EmbeddedChannel channel = connectWithCompression(server);

        opened.getFirst().sendMessage("Hello");

        WebSocketFrame frame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(TextWebSocketFrame.class)).actual();
        assertThat(frame.rsv()).as("Should mark the message as compressed").isEqualTo(RSV1);
        assertThat(decompress(frame.content())).as("Should send the compressed message").isEqualTo("Hello");
        frame.release();
    }

    @Test
    void shouldCloseConnectionWith1009WhenCompressedMessageExpandsPastMaxMessageSize() {
        List<String> received = new ArrayList<>();
        server
            .configure(configurer -> configurer
                .setMaxFrameSize(1024)
                .setMaxMessageSize(1024)
            )
            .onMessage((_, message) -> received.add(message));

        EmbeddedChannel channel = connectWithCompression(server);

        // Send a message that compresses to a few bytes, but inflates to ten times the limit
        Util.sendFromClient(channel, new TextWebSocketFrame(true, RSV1, compress("a".repeat(10 * 1024))));

        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send message too big status").isEqualTo(WebSocketCloseStatus.MESSAGE_TOO_BIG.code());
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(received).as("Message should not be delivered").isEmpty();
        closeFrame.release();
    }

    @Test
    void shouldCloseConnectionWith1007WhenCompressedMessageIsCorrupted() {
        List<String> received = new ArrayList<>();
        server.onMessage((_, message) -> received.add(message));

        EmbeddedChannel channel = connectWithCompression(server);

        // 0xff starts a DEFLATE block of the reserved type 11, which cannot be inflated
        Util.sendFromClient(channel, new TextWebSocketFrame(true, RSV1, Unpooled.wrappedBuffer(new byte[]{(byte) 0xff, (byte) 0xff})));

        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send invalid payload data status").isEqualTo(WebSocketCloseStatus.INVALID_PAYLOAD_DATA.code());
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(received).as("Message should not be delivered").isEmpty();
        closeFrame.release();
    }

    // RSV1, RSV2 and RSV3, the decoder lets them through once compression is enabled
    @ParameterizedTest
    @ValueSource(ints = {0b100, 0b010, 0b001})
    void shouldCloseConnectionWithProtocolErrorWhenFrameHasReservedBitSetWithoutNegotiatedCompression(int rsv) {
        List<String> received = new ArrayList<>();
        server.onMessage((_, message) -> received.add(message));

        // Connect without offering compression
        EmbeddedChannel channel = Util.connect(server);

        Util.sendFromClient(channel, new TextWebSocketFrame(true, rsv, Unpooled.copiedBuffer("Hello", StandardCharsets.UTF_8)));

        // Assert connection was failed with a protocol error close frame
        CloseWebSocketFrame closeFrame = assertThat(Util.readFromServer(channel)).asInstanceOf(type(CloseWebSocketFrame.class)).actual();
        assertThat(closeFrame.statusCode()).as("Should send protocol error status").isEqualTo(WebSocketCloseStatus.PROTOCOL_ERROR.code());
        assertThat(channel.isOpen()).as("Channel should be closed").isFalse();
        assertThat(received).as("Message should not be delivered").isEmpty();
        closeFrame.release();
    }
}
