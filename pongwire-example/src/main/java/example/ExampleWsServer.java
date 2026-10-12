package example;

import pl.mbaracz.pongwire.TlsConfiguration;
import pl.mbaracz.pongwire.UpgradeRequest;
import pl.mbaracz.pongwire.WebSocketServer;
import pl.mbaracz.pongwire.configuration.BackpressurePolicy;
import pl.mbaracz.pongwire.handler.UpgradeResult;
import pl.mbaracz.pongwire.message.impl.json.JsonMessageDecoder;
import pl.mbaracz.pongwire.message.impl.json.JsonMessageEncoder;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Example WebSocket server with TLS, JSON and bearer token authentication.
 */
public final class ExampleWsServer {

    private static final System.Logger LOGGER = System.getLogger(ExampleWsServer.class.getName());
    private static final int PORT = 8443;

    private ExampleWsServer() {
    }

    static void main() {
        String authToken = requiredEnvironment("AUTH_TOKEN");
        Path certificate = Path.of(requiredEnvironment("TLS_CERTIFICATE"));
        Path privateKey = Path.of(requiredEnvironment("TLS_PRIVATE_KEY"));

        ExecutorService callbackExecutor = Executors.newVirtualThreadPerTaskExecutor();
        var server = new WebSocketServer<ChatMessage, SessionContext>("/chat")
            .configure(config -> config
                .setTlsConfiguration(TlsConfiguration.forPem(certificate, privateKey))
                .setMessageDecoder(new JsonMessageDecoder<>(ChatMessage.class))
                .setMessageEncoder(new JsonMessageEncoder<>())
                .setHeartbeatInterval(Duration.ofSeconds(30))
                .setHeartbeatTimeout(Duration.ofSeconds(10))
                .setIdleTimeout(Duration.ofMinutes(5))
                .setWriteBufferWaterMark(32 * 1024, 64 * 1024)
                .setBackpressurePolicy(BackpressurePolicy.REJECT_NEW)
                .setUnwritableTimeout(Duration.ofSeconds(15))
                .setCallbackExecutor(callbackExecutor)
                .setCloseOnException(true)
            )
            .onUpgrade((request, response) -> {
                if (!hasBearerToken(request, authToken)) {
                    response.setStatus(401).setHeader("WWW-Authenticate", "Bearer");
                    return UpgradeResult.reject();
                }

                return UpgradeResult.accept(new SessionContext("api-client", request.getRemoteAddress()));
            })
            .onOpen(session -> LOGGER.log(
                System.Logger.Level.INFO,
                "Connected {0} from {1}",
                session.getContext().subject(),
                session.getContext().remoteAddress()
            ))
            .onMessage((session, message) -> session
                .sendMessageAsync(new ChatMessage(session.getContext().subject(), message.text()))
                .exceptionally(error -> {
                    LOGGER.log(System.Logger.Level.WARNING, "Could not send message", error);
                    return null;
                }))
            .onWritabilityChanged((session, writable) -> LOGGER.log(
                System.Logger.Level.INFO,
                "Client {0} writable: {1}",
                session.getContext().subject(),
                writable
            ))
            .onError((session, error) ->
                LOGGER.log(System.Logger.Level.ERROR, "WebSocket error", error)
            )
            .onClose((session, reason, code) -> LOGGER.log(
                System.Logger.Level.INFO,
                "Disconnected {0}: {1} ({2})",
                session.getContext().subject(),
                reason,
                code
            ));

        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform()
            .name("websocket-shutdown")
            .unstarted(() -> shutdown(server, callbackExecutor)));

        try {
            server.listen(PORT);
        } catch (RuntimeException exception) {
            callbackExecutor.close();
            throw exception;
        }
    }

    private static boolean hasBearerToken(UpgradeRequest request, String expectedToken) {
        String authorization = request.getHeader("Authorization");
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            return false;
        }

        byte[] provided = authorization.substring("Bearer ".length()).getBytes(StandardCharsets.UTF_8);
        byte[] expected = expectedToken.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(provided, expected);
    }

    private static void shutdown(
        WebSocketServer<ChatMessage, SessionContext> server,
        ExecutorService callbackExecutor
    ) {
        try {
            if (server.isRunning()) {
                server.stop();
            }
        } finally {
            callbackExecutor.close();
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing environment variable: " + name);
        }
        return value;
    }

    public record ChatMessage(String author, String text) {
    }

    public record SessionContext(String subject, InetSocketAddress remoteAddress) {
    }
}
