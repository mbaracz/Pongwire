package pl.mbaracz.pongwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocket13FrameDecoder;
import io.netty.handler.codec.http.websocketx.WebSocket13FrameEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrameDecoder;
import io.netty.handler.codec.http.websocketx.WebSocketFrameEncoder;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

class Util {

    private static final Queue<EmbeddedChannel> EMBEDDED_CHANNELS = new ConcurrentLinkedQueue<>();

    static EmbeddedChannel newEmbeddedChannel(ChannelHandler... handlers) {
        EmbeddedChannel channel = new EmbeddedChannel(handlers);
        EMBEDDED_CHANNELS.add(channel);
        return channel;
    }

    static EmbeddedChannel newEmbeddedChannel(ChannelId id, ChannelHandler... handlers) {
        EmbeddedChannel channel = new EmbeddedChannel(id, handlers);
        EMBEDDED_CHANNELS.add(channel);
        return channel;
    }

    static void finishAndReleaseEmbeddedChannels() {
        EmbeddedChannel channel;

        while ((channel = EMBEDDED_CHANNELS.poll()) != null) {
            channel.finishAndReleaseAll();
        }
    }

    static FullHttpRequest createHttpRequest(String path) {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
        HttpHeaders headers = getDefaultHeaders();
        request.headers().set(headers);
        return request;
    }

    static void performHandshake(EmbeddedChannel channel, String path) {
        FullHttpRequest request = createHttpRequest(path);
        channel.writeInbound(request);
    }

    static void completeHandshake(EmbeddedChannel channel, String path) {
        channel.pipeline().addFirst(new HttpServerCodec());
        performHandshake(channel, path);

        // Discard the 101 Switching Protocols response
        channel.releaseOutbound();

        // Exchange WebSocket frames as objects instead of encoded bytes
        channel.pipeline().remove(WebSocketFrameEncoder.class);
        channel.pipeline().remove(WebSocketFrameDecoder.class);
    }

    /**
     * Creates a channel with the server's pipeline and completes the WebSocket handshake.
     */
    static <T, D> EmbeddedChannel connect(WebSocketServer<T, D> server) {
        EmbeddedChannel channel = newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));
        performHandshake(channel, "/");

        // Discard the 101 Switching Protocols response
        channel.releaseOutbound();

        return channel;
    }

    /**
     * Encodes the frames like a client does (masked) and writes them to the server in a single read.
     */
    static void sendFromClient(EmbeddedChannel channel, WebSocketFrame... frames) {
        EmbeddedChannel client = new EmbeddedChannel(new WebSocket13FrameEncoder(true));
        try {
            client.writeOutbound((Object[]) frames);

            List<ByteBuf> encoded = new ArrayList<>();
            ByteBuf buffer;

            while ((buffer = client.readOutbound()) != null) {
                encoded.add(buffer);
            }

            channel.writeInbound(Unpooled.wrappedBuffer(encoded.toArray(ByteBuf[]::new)));
        } finally {
            client.finishAndReleaseAll();
        }
    }

    /**
     * Decodes the frames written by the server and returns the first one.
     */
    static WebSocketFrame readFromServer(EmbeddedChannel channel) {
        EmbeddedChannel client = new EmbeddedChannel(new WebSocket13FrameDecoder(false, true, 65536));
        try {
            ByteBuf buffer;

            while ((buffer = channel.readOutbound()) != null) {
                client.writeInbound(buffer);
            }

            return client.readInbound();
        } finally {
            client.finishAndReleaseAll();
        }
    }

    static HttpHeaders getDefaultHeaders() {
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.HOST, "localhost:8081");
        headers.add(HttpHeaderNames.UPGRADE, "websocket");
        headers.add(HttpHeaderNames.CONNECTION, "Upgrade");
        headers.add(HttpHeaderNames.SEC_WEBSOCKET_KEY, Base64.getEncoder().encodeToString("random16ByteKey!".getBytes()));
        headers.add(HttpHeaderNames.SEC_WEBSOCKET_VERSION, "13");
        return headers;
    }
}
