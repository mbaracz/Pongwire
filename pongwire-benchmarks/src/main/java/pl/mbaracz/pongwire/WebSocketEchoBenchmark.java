package pl.mbaracz.pongwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocket13FrameEncoder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;

/**
 * Measures a text message echoed by the server's pipeline, from the client's encoded frame to the server's encoded reply.
 * It runs on an embedded channel, so the results leave out the network and the event loop.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class WebSocketEchoBenchmark {

    // Marks a compressed message (RFC 7692, section 6)
    private static final int RSV1 = 0b100;

    // Empty block that ends a compressed message, the sender removes it (RFC 7692, section 7.2.1)
    private static final int MESSAGE_TAIL_LENGTH = 4;

    // The messages together exceed the 32 KiB deflate window, so the server cannot compress a message
    // as a reference to its earlier copy
    private static final int MIN_MESSAGES_BYTES = 1024 * 1024;
    private static final int MIN_MESSAGES = 16;

    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz ";

    @Param({"128", "4096", "65536"})
    private int payloadSize;

    @Param({"false", "true"})
    private boolean compression;

    private EmbeddedChannel channel;
    private byte[][] frames;
    private int next;

    @Setup(Level.Trial)
    public void setUp() {
        WebSocketServer<String, Object> server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                .setCompressionEnabled(compression)
            )
            .onMessage(WebSocketSession::sendMessage);

        channel = new EmbeddedChannel(new WebSocketServerChannelInitializer<>(server));
        handshake();

        SplittableRandom random = new SplittableRandom(42);
        frames = new byte[Math.max(MIN_MESSAGES, MIN_MESSAGES_BYTES / payloadSize)][];

        for (int i = 0; i < frames.length; i++) {
            frames[i] = encodeClientFrame(randomText(random, payloadSize));
        }

        if (echo() == 0) {
            throw new IllegalStateException("Server did not echo the message");
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Benchmark
    public int echo() {
        byte[] frame = frames[next];
        next = (next + 1) % frames.length;

        // Copy the frame to a new buffer like a socket read does, the decoder unmasks the payload in place
        channel.writeInbound(channel.alloc().buffer(frame.length).writeBytes(frame));

        int written = 0;
        ByteBuf buffer;

        while ((buffer = channel.readOutbound()) != null) {
            written += buffer.readableBytes();
            buffer.release();
        }
        return written;
    }

    /**
     * Completes the handshake, offering compression when it is enabled, and checks that the server accepted it.
     */
    private void handshake() {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        request.headers()
            .set(HttpHeaderNames.HOST, "localhost")
            .set(HttpHeaderNames.UPGRADE, "websocket")
            .set(HttpHeaderNames.CONNECTION, "Upgrade")
            .set(HttpHeaderNames.SEC_WEBSOCKET_KEY, Base64.getEncoder().encodeToString("benchmark-key-16".getBytes(StandardCharsets.US_ASCII)))
            .set(HttpHeaderNames.SEC_WEBSOCKET_VERSION, "13");

        if (compression) {
            request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS, "permessage-deflate");
        }

        channel.writeInbound(request);

        EmbeddedChannel client = new EmbeddedChannel(new HttpResponseDecoder());
        ByteBuf buffer;

        while ((buffer = channel.readOutbound()) != null) {
            client.writeInbound(buffer);
        }

        HttpResponse response = client.readInbound();
        client.finishAndReleaseAll();

        if (response == null || !HttpResponseStatus.SWITCHING_PROTOCOLS.equals(response.status())) {
            throw new IllegalStateException("Handshake failed: " + response);
        }

        String extensions = response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS);
        if (compression != "permessage-deflate".equals(extensions)) {
            throw new IllegalStateException("Unexpected extensions: " + extensions);
        }
    }

    /**
     * Encodes the text as a masked client frame, compressed when compression is enabled.
     */
    private byte[] encodeClientFrame(String text) {
        byte[] payload = text.getBytes(StandardCharsets.UTF_8);
        TextWebSocketFrame frame = compression
            ? new TextWebSocketFrame(true, RSV1, Unpooled.wrappedBuffer(compress(payload)))
            : new TextWebSocketFrame(Unpooled.wrappedBuffer(payload));

        EmbeddedChannel client = new EmbeddedChannel(new WebSocket13FrameEncoder(true));
        client.writeOutbound(frame);

        ByteBuf encoded = Unpooled.buffer();

        try {
            ByteBuf buffer;

            while ((buffer = client.readOutbound()) != null) {
                encoded.writeBytes(buffer);
                buffer.release();
            }

            return ByteBufUtil.getBytes(encoded);
        } finally {
            encoded.release();
            client.finishAndReleaseAll();
        }
    }

    /**
     * Compresses the payload the way a client compresses a message (RFC 7692, section 7.2.1).
     */
    private static byte[] compress(byte[] payload) {
        Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        deflater.setInput(payload);

        byte[] buffer = new byte[payload.length + 1024];
        int length = deflater.deflate(buffer, 0, buffer.length, Deflater.SYNC_FLUSH);
        deflater.end();

        // The sync flush ends the data with the empty block, which is not sent
        return Arrays.copyOf(buffer, length - MESSAGE_TAIL_LENGTH);
    }

    private static String randomText(SplittableRandom random, int length) {
        StringBuilder text = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            text.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
        }
        return text.toString();
    }
}
