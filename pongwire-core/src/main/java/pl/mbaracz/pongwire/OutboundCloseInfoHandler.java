package pl.mbaracz.pongwire;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;

/**
 * Records the first outgoing close frame before the WebSocket encoder consumes it.
 */
final class OutboundCloseInfoHandler extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
        if (message instanceof CloseWebSocketFrame frame) {
            int code = frame.statusCode() == -1 ? WebSocketCloseStatus.EMPTY.code() : frame.statusCode();
            context.channel().attr(CloseInfo.KEY).setIfAbsent(new CloseInfo(code, frame.reasonText()));
            ClosingHandshake.markServerStarted(context.channel());
        }

        context.write(message, promise);
    }
}
