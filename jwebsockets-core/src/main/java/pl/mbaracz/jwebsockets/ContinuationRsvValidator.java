package pl.mbaracz.jwebsockets;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketExtension;

/**
 * Fails the connection with 1002 when a continuation frame has the RSV1 bit set.
 * Only the first frame of a compressed message carries it (RFC 7692, section 6.1).
 * Placed before the compression decoder, which clears the bit on continuations without checking it.
 */
class ContinuationRsvValidator extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
        if (message instanceof ContinuationWebSocketFrame frame && (frame.rsv() & WebSocketExtension.RSV1) != 0) {
            frame.release();
            context.writeAndFlush(new CloseWebSocketFrame(WebSocketCloseStatus.PROTOCOL_ERROR))
                .addListener(ChannelFutureListener.CLOSE);
            return;
        }

        context.fireChannelRead(message);
    }
}
