package pl.mbaracz.pongwire;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

/**
 * Fails the connection with 1002 when a frame has a reserved bit set that no negotiated extension used
 * (RFC 6455, section 5.2). Placed after the compression decoder, which clears the RSV1 bit of the messages it decompresses.
 */
class ReservedBitsValidator extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
        if (message instanceof WebSocketFrame frame && frame.rsv() != 0) {
            frame.release();
            context.writeAndFlush(new CloseWebSocketFrame(WebSocketCloseStatus.PROTOCOL_ERROR))
                .addListener(ChannelFutureListener.CLOSE);
            return;
        }

        context.fireChannelRead(message);
    }
}
