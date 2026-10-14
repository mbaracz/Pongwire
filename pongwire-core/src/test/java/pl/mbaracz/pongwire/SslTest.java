package pl.mbaracz.pongwire;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pl.mbaracz.pongwire.handler.UpgradeResult;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageDecoder;
import pl.mbaracz.pongwire.message.impl.plain.PlainTextMessageEncoder;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.TrustManagerFactory;

import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SslTest {

    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(5);
    private static final long CALLBACK_TIMEOUT_SECONDS = 5;

    private static SelfSignedCertificate certificate;
    private static TlsConfiguration serverTlsConfiguration;
    private static SSLContext trustedClientSslContext;
    private static boolean installedBouncyCastle;

    private WebSocketServer<String, Object> server;
    private HttpClient client;
    private WebSocket webSocket;

    @BeforeAll
    static void createCertificate() throws Exception {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
            installedBouncyCastle = true;
        }

        certificate = new SelfSignedCertificate("localhost");
        serverTlsConfiguration = TlsConfiguration.forPem(
            certificate.certificate().toPath(),
            certificate.privateKey().toPath()
        );
        trustedClientSslContext = createClientSslContext(certificate);
    }

    @AfterAll
    static void deleteCertificate() {
        certificate.delete();
        if (installedBouncyCastle) {
            Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
        }
    }

    @AfterEach
    void tearDown() {
        if (webSocket != null) {
            webSocket.abort();
        }
        if (client != null) {
            // close() would wait for a connection a failing test left open
            client.shutdownNow();
        }
        if (server != null && server.isRunning()) {
            server.stop();
        }
    }

    @Test
    void shouldExchangeMessagesAndCloseCleanlyOverWss() throws Exception {
        CountDownLatch opened = new CountDownLatch(1);
        CountDownLatch serverReceived = new CountDownLatch(1);
        CountDownLatch clientReceived = new CountDownLatch(1);
        CountDownLatch serverClosed = new CountDownLatch(1);
        CountDownLatch clientClosed = new CountDownLatch(1);
        AtomicReference<String> messageReceivedByServer = new AtomicReference<>();
        AtomicReference<String> messageReceivedByClient = new AtomicReference<>();
        AtomicReference<Throwable> clientFailure = new AtomicReference<>();

        server = newServer(serverTlsConfiguration)
            .onOpen(_ -> opened.countDown())
            .onMessage((session, message) -> {
                messageReceivedByServer.set(message);
                session.sendMessage("world");
                serverReceived.countDown();
            })
            .onClose((_, _, _) -> serverClosed.countDown())
            .listen(0);

        client = newClient(trustedClientSslContext);
        webSocket = client.newWebSocketBuilder()
            .connectTimeout(CLIENT_TIMEOUT)
            .buildAsync(serverUri(), new WebSocket.Listener() {
                private final StringBuilder text = new StringBuilder();

                @Override
                public void onOpen(WebSocket socket) {
                    socket.request(1);
                }

                @Override
                public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) {
                    text.append(data);
                    if (last) {
                        messageReceivedByClient.set(text.toString());
                        clientReceived.countDown();
                    }
                    socket.request(1);
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public CompletionStage<?> onClose(WebSocket socket, int statusCode, String reason) {
                    clientClosed.countDown();
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public void onError(WebSocket socket, Throwable error) {
                    clientFailure.set(error);
                    clientReceived.countDown();
                    clientClosed.countDown();
                }
            })
            .get(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(opened.await(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS)).as("Server onOpen callback").isTrue();

        webSocket.sendText("hello", true).get(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(serverReceived.await(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS)).as("Message received by server").isTrue();
        assertThat(clientReceived.await(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS)).as("Message received by client").isTrue();
        assertThat(clientFailure).as("Client failure").hasNullValue();
        assertThat(messageReceivedByServer).hasValue("hello");
        assertThat(messageReceivedByClient).hasValue("world");

        webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(clientClosed.await(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS)).as("Client close callback").isTrue();
        assertThat(serverClosed.await(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS)).as("Server close callback").isTrue();
        assertThat(server.getConnectedSessions()).as("Sessions after close").isEmpty();
    }

    @Test
    void shouldRejectUntrustedCertificateBeforeOpeningWebSocketSession() {
        AtomicBoolean opened = new AtomicBoolean();

        server = newServer(serverTlsConfiguration)
            .onOpen(_ -> opened.set(true))
            .listen(0);
        client = newClient(null);

        CompletableFuture<WebSocket> connection = client.newWebSocketBuilder()
            .connectTimeout(CLIENT_TIMEOUT)
            .buildAsync(serverUri(), new WebSocket.Listener() {});

        assertThatThrownBy(() -> connection.get(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS))
            .hasCauseInstanceOf(SSLHandshakeException.class);
        assertThat(opened).as("Server onOpen callback").isFalse();
        assertThat(server.getConnectedSessions()).as("Sessions after failed TLS handshake").isEmpty();
    }

    @Test
    void shouldPassClientCertificateToUpgradeHandlerWhenContextRequiresClientAuthentication() throws Exception {
        SelfSignedCertificate clientCertificate = new SelfSignedCertificate("client");
        try {
            AtomicReference<String> clientSubject = new AtomicReference<>();
            server = newServer(TlsConfiguration.forSslContext(createSslContext(certificate, clientCertificate), requireClientAuth()))
                .onUpgrade((request, _) -> {
                    try {
                        X509Certificate peer = (X509Certificate) request.getSslSession().getPeerCertificates()[0];
                        clientSubject.set(peer.getSubjectX500Principal().getName());
                    } catch (SSLPeerUnverifiedException exception) {
                        clientSubject.set("unverified");
                    }
                    return UpgradeResult.accept(null);
                })
                .listen(0);

            client = newClient(createClientSslContext(certificate, clientCertificate));
            webSocket = client.newWebSocketBuilder()
                .connectTimeout(CLIENT_TIMEOUT)
                .buildAsync(serverUri(), new WebSocket.Listener() {})
                .get(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertThat(clientSubject).as("Client certificate subject").hasValue("CN=client");
        } finally {
            clientCertificate.delete();
        }
    }

    @Test
    void shouldRejectClientWithoutCertificateWhenContextRequiresClientAuthentication() throws Exception {
        server = newServer(TlsConfiguration.forSslContext(createSslContext(certificate, certificate), requireClientAuth())).listen(0);
        client = newClient(trustedClientSslContext);

        CompletableFuture<WebSocket> connection = client.newWebSocketBuilder()
            .connectTimeout(CLIENT_TIMEOUT)
            .buildAsync(serverUri(), new WebSocket.Listener() {});

        assertThatThrownBy(() -> connection.get(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
        assertThat(server.getConnectedSessions()).as("Sessions").isEmpty();
    }

    @Test
    void shouldCloseConnectionsPastConcurrentTlsHandshakeLimit() throws Exception {
        server = newServer(serverTlsConfiguration)
            .configure(configurer -> configurer.setMaxConcurrentTlsHandshakes(1))
            .listen(0);

        // Clients that send no ClientHello keep their TLS handshake in progress. The event loops accept the
        // connections in either order, so one of them takes the slot and the other one is closed.
        try (Socket first = new Socket("127.0.0.1", server.getLocalAddress().getPort());
             Socket second = new Socket("127.0.0.1", server.getLocalAddress().getPort())) {
            int closed = 0;

            for (Socket socket : List.of(first, second)) {
                socket.setSoTimeout(2000);
                try {
                    if (socket.getInputStream().read() == -1) {
                        closed++;
                    }
                } catch (SocketTimeoutException exception) {
                    // The pending handshake stays open
                }
            }

            assertThat(closed).as("Exactly one connection should be past the limit").isEqualTo(1);
        }
    }

    @Test
    void shouldNotInstallSslHandlerWhenSslIsDisabled() {
        server = newServer(null);

        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));

        assertThat(channel.pipeline().context(SslHandler.class)).isNull();
        assertThat(channel.pipeline().context(HttpRequestDecoder.class)).isNotNull();
    }

    @Test
    void shouldInstallSslHandlerBeforeHttpCodecWhenSslIsEnabled() {
        server = newServer(serverTlsConfiguration);

        EmbeddedChannel channel = Util.newEmbeddedChannel(new WebSocketServerChannelInitializer<>(server));
        ChannelPipeline pipeline = channel.pipeline();
        String sslHandler = pipeline.context(SslHandler.class).name();
        String httpCodec = pipeline.context(HttpRequestDecoder.class).name();
        List<String> handlerNames = pipeline.names();

        assertThat(handlerNames.indexOf(sslHandler)).isLessThan(handlerNames.indexOf(httpCodec));
    }

    private WebSocketServer<String, Object> newServer(TlsConfiguration tlsConfiguration) {
        return new WebSocketServer<String, Object>()
            .configure(configurer -> configurer
                .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
                .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
                .setTlsConfiguration(tlsConfiguration)
            );
    }

    private HttpClient newClient(SSLContext sslContext) {
        HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(CLIENT_TIMEOUT);
        if (sslContext != null) {
            builder.sslContext(sslContext);
        }
        return builder.build();
    }

    private URI serverUri() {
        return URI.create("wss://localhost:" + server.getLocalAddress().getPort() + "/");
    }

    private static SSLContext createClientSslContext(SelfSignedCertificate trustedCertificate) throws Exception {
        return createSslContext(null, trustedCertificate);
    }

    private static SSLContext createClientSslContext(
        SelfSignedCertificate trustedCertificate,
        SelfSignedCertificate clientCertificate
    ) throws Exception {
        return createSslContext(clientCertificate, trustedCertificate);
    }

    private static SSLParameters requireClientAuth() {
        SSLParameters parameters = new SSLParameters();
        parameters.setNeedClientAuth(true);
        return parameters;
    }

    /**
     * Creates a context that authenticates with its own certificate, if given, and trusts the other one.
     */
    private static SSLContext createSslContext(
        SelfSignedCertificate ownCertificate,
        SelfSignedCertificate trustedCertificate
    ) throws Exception {
        KeyManagerFactory keyManagerFactory = null;
        if (ownCertificate != null) {
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);
            keyStore.setKeyEntry("own", ownCertificate.key(), new char[0], new Certificate[]{ownCertificate.cert()});
            keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, new char[0]);
        }

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("server", trustedCertificate.cert());

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagerFactory == null ? null : keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
        return context;
    }
}
