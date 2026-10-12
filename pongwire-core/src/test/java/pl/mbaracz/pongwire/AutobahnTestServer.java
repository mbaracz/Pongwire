package pl.mbaracz.pongwire;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import pl.mbaracz.pongwire.handler.UpgradeResult;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Echo server tested by the Autobahn WebSocket Testsuite, see {@code autobahn/README.md}.
 * Autobahn expects every message back with the same payload and the same frame type: text as text, binary as binary.
 */
public final class AutobahnTestServer {

    private static final int PORT = 9001;

    /**
     * Frame type of the message being handled, kept in the session context.
     */
    private enum MessageType {
        TEXT,
        BINARY
    }

    static void main() {
        WebSocketServer<byte[], AtomicReference<MessageType>> server = new WebSocketServer<byte[], AtomicReference<MessageType>>()
            .configure(configurer -> configurer
                .setMessageDecoder(data -> data)
                .setMessageEncoder(message -> message)
                .setAllowBinaryFrames(true)
                .setCompressionEnabled(true)
            )
            .onUpgrade((_, _) -> UpgradeResult.accept(new AtomicReference<>()))
            .onOpen(AutobahnTestServer::recordMessageTypes)
            .onMessage(AutobahnTestServer::echo);

        server.listen(PORT);
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(server::stop));

        System.out.println("Pongwire listening on :" + PORT);
    }

    /**
     * Stores the frame type of every message in the session context before the server's
     * handler decodes it, because the message handler receives only the payload.
     */
    private static void recordMessageTypes(WebSocketSession<byte[], AtomicReference<MessageType>> session) {
        ChannelHandlerContext serverHandler = session.getChannelContext();

        serverHandler.pipeline().addBefore(serverHandler.name(), "autobahnMessageType", new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext context, Object message) {
                if (message instanceof TextWebSocketFrame) {
                    session.getContext().set(MessageType.TEXT);
                } else if (message instanceof BinaryWebSocketFrame) {
                    session.getContext().set(MessageType.BINARY);
                }
                context.fireChannelRead(message);
            }
        });
    }

    /**
     * Sends the message back in a frame of the type it arrived in.
     * The server's handler calls this on the event loop right after
     * the frame passed the recorder, so the session context describes this message.
     */
    private static void echo(WebSocketSession<byte[], AtomicReference<MessageType>> session, byte[] payload) {
        WebSocketFrame frame = switch (session.getContext().get()) {
            case TEXT -> new TextWebSocketFrame(Unpooled.wrappedBuffer(payload));
            case BINARY -> new BinaryWebSocketFrame(Unpooled.wrappedBuffer(payload));
        };

        // sendMessage() would use the frame type configured for the whole server, so the frame is written directly
        session.getChannelContext().writeAndFlush(frame);
    }
}
