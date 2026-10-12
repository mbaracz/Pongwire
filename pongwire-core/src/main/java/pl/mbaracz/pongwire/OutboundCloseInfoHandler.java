package pl.mbaracz.pongwire;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;

import java.time.Duration;

/**
 * Records the first outgoing close frame before the WebSocket encoder consumes it.
 * A close frame written by a validator closes the connection only once it is written, which a client that stopped
 * reading can delay forever, so the first close frame also starts the close timeout.
 */
final class OutboundCloseInfoHandler extends ChannelOutboundHandlerAdapter {

    private final Duration closeTimeout;

    OutboundCloseInfoHandler(Duration closeTimeout) {
        this.closeTimeout = closeTimeout;
    }

    @Override
    public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
        if (message instanceof CloseWebSocketFrame frame) {
            int code = frame.statusCode() == -1 ? WebSocketCloseStatus.EMPTY.code() : frame.statusCode();
            context.channel().attr(CloseInfo.KEY).setIfAbsent(new CloseInfo(code, frame.reasonText()));

            // The closing handshake started by the server schedules its own timeout before it writes the frame
            if (ClosingHandshake.markServerStarted(context.channel())) {
                ClosingHandshake.closeAfter(context.channel(), closeTimeout);
            }
        }

        context.write(message, promise);
    }
}
