package pl.mbaracz.jwebsockets;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.jwebsockets.message.impl.plain.PlainTextMessageEncoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionLimitTest {

    private final List<Socket> sockets = new ArrayList<>();
    private WebSocketServer<String, Object> server;

    @BeforeEach
    void setUp() {
        server = new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
            );
    }

    @AfterEach
    void tearDown() throws IOException {
        for (Socket socket : sockets) {
            socket.close();
        }
        if (server.isRunning()) {
            server.stop();
        }
    }

    private Socket connect() throws IOException {
        Socket socket = new Socket();
        sockets.add(socket);
        socket.connect(new InetSocketAddress("127.0.0.1", server.getLocalAddress().getPort()), 2000);
        socket.setSoTimeout(500);
        return socket;
    }

    /**
     * A connection the server keeps open does not answer before the client sends its request, so the read times out.
     */
    private static boolean isClosedByServer(Socket socket) throws IOException {
        try {
            return socket.getInputStream().read() == -1;
        } catch (SocketTimeoutException exception) {
            return false;
        }
    }

    @Test
    void shouldCloseConnectionsPastGlobalLimitAndAcceptAgainOnceOneCloses() throws IOException {
        server.configure(configurer -> configurer.setMaxConnections(1)).listen(0);

        Socket first = connect();
        assertThat(isClosedByServer(first)).as("First connection should stay open").isFalse();
        assertThat(isClosedByServer(connect())).as("Connection past the limit should be closed").isTrue();

        first.close();

        assertThat(isAcceptedWithin2Seconds()).as("Connection should be accepted again").isTrue();
    }

    /**
     * The server releases the slot of a closed connection on its event loop, shortly after the client closed it.
     */
    private boolean isAcceptedWithin2Seconds() throws IOException {
        long deadline = System.nanoTime() + 2_000_000_000L;

        while (System.nanoTime() < deadline) {
            if (!isClosedByServer(connect())) {
                return true;
            }
        }
        return false;
    }

    @Test
    void shouldCloseConnectionsPastPerAddressLimit() throws IOException {
        server.configure(configurer -> configurer.setMaxConnectionsPerAddress(2)).listen(0);

        assertThat(isClosedByServer(connect())).as("First connection").isFalse();
        assertThat(isClosedByServer(connect())).as("Second connection").isFalse();
        assertThat(isClosedByServer(connect())).as("Third connection from the same address").isTrue();
    }

    @Test
    void shouldAcceptConnectionsWithoutLimit() throws IOException {
        server.listen(0);

        for (int i = 0; i < 5; i++) {
            assertThat(isClosedByServer(connect())).as("Connection " + i).isFalse();
        }
    }
}
