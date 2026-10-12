package pl.mbaracz.pongwire;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;

/**
 * Fails the connection with 1002 when a frame has a reserved bit set that no negotiated extension used
 * (RFC 6455, section 5.2). Placed after the compression decoder, which clears the RSV1 bit of the messages it decompresses.
 * The frames after the failing one are dropped, so the connection fails once.
 */
class ReservedBitsValidator extends ChannelInboundHandlerAdapter {

    private boolean failed;

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
        if (failed) {
            ReferenceCountUtil.release(message);
            return;
        }

        if (message instanceof WebSocketFrame frame && frame.rsv() != 0) {
            failed = true;
            frame.release();
            context.writeAndFlush(new CloseWebSocketFrame(WebSocketCloseStatus.PROTOCOL_ERROR))
                .addListener(ChannelFutureListener.CLOSE);
            return;
        }

        context.fireChannelRead(message);
    }
}
