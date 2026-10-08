package pl.mbaracz.jwebsockets;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;

/**
 * Holds back read requests while the server stopped reading from the connection.
 * Disabling autoRead alone does not stop handlers such as the frame aggregator,
 * which request a read themselves while a fragmented message is incomplete.
 * Reading resumes by enabling autoRead again, which requests a read.
 */
final class ReadGate extends ChannelOutboundHandlerAdapter {

    private boolean closed;

    @Override
    public void read(ChannelHandlerContext context) {
        if (!closed) {
            context.read();
        }
    }

    /**
     * Stops or resumes reading from the connection. Must be called on the event loop.
     *
     * @param context the context of any handler of the connection.
     * @param reading whether to read from the connection.
     */
    static void setReading(ChannelHandlerContext context, boolean reading) {
        ReadGate gate = context.pipeline().get(ReadGate.class);

        if (gate != null) {
            gate.closed = !reading;
        }

        context.channel().config().setAutoRead(reading);
    }
}
