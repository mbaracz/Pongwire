package pl.mbaracz.pongwire;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.http.HttpDecoderConfig;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.Utf8FrameValidator;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketExtensionData;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketExtensionFilter;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketExtensionFilterProvider;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketServerExtensionHandler;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketServerExtensionHandshaker;
import io.netty.handler.codec.http.websocketx.extensions.compression.PerMessageDeflateServerExtensionHandshaker;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import pl.mbaracz.pongwire.configuration.WebSocketServerConfiguration;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import java.io.File;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Channel initializer for setting up the pipeline for WebSocket server channels.
 *
 * @param <T> the type of WebSocket messages.
 * @param <D> the type of additional data associated with WebSocket sessions.
 */
class WebSocketServerChannelInitializer<T, D> extends ChannelInitializer<Channel> {

    private static final String SERVER_MAX_WINDOW_BITS = "server_max_window_bits";
    private static final String SERVER_NO_CONTEXT_TAKEOVER = "server_no_context_takeover";

    private final WebSocketServer<T, D> webSocketServer;
    private final SslContext sslContext;
    private final SSLParameters sslParameters;
    private final AtomicInteger tlsHandshakes = new AtomicInteger();

    /**
     * Constructs a WebSocketServerChannelInitializer with the provided WebSocket server.
     *
     * @param webSocketServer the WebSocket server instance.
     */
    WebSocketServerChannelInitializer(WebSocketServer<T, D> webSocketServer) {
        this.webSocketServer = webSocketServer;
        TlsConfiguration tlsConfiguration = webSocketServer.getConfiguration().getTlsConfiguration();
        this.sslContext = createSslContext(tlsConfiguration);
        this.sslParameters = tlsConfiguration == null ? null : tlsConfiguration.sslParameters();
    }

    /**
     * Initializes the channel pipeline with the necessary handlers for WebSocket communication.
     *
     * @param channel the socket channel being initialized.
     */
    @Override
    protected void initChannel(Channel channel) {
        // Before any handler is added, so a connection past the limit costs no pipeline
        if (!webSocketServer.acquireConnection(channel)) {
            channel.close();
            return;
        }

        ChannelPipeline pipeline = channel.pipeline();

        WebSocketServerConfiguration<T> configuration = webSocketServer.getConfiguration();

        Integer lowWaterMark = configuration.getWriteBufferLowWaterMark();
        Integer highWaterMark = configuration.getWriteBufferHighWaterMark();
        if (lowWaterMark != null) {
            channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(lowWaterMark, highWaterMark));
        }

        // Closest to the transport, so it also holds back the reads requested by the handlers after it
        pipeline.addLast(new ReadGate());

        if (sslContext != null) {
            if (!acquireTlsHandshake(configuration.getMaxConcurrentTlsHandshakes())) {
                channel.close();
                return;
            }

            SslHandler sslHandler = sslContext.newHandler(channel.alloc());
            // The server's handshake timeout runs from the accept and covers the TLS handshake too, so Netty's fixed
            // 10 seconds would only close connections early during a reconnect storm
            sslHandler.setHandshakeTimeoutMillis(0);
            if (sslParameters != null) {
                sslHandler.engine().setSSLParameters(sslParameters);
            }
            pipeline.addLast(sslHandler);

            if (configuration.getMaxConcurrentTlsHandshakes() > 0) {
                releaseTlsHandshakeWhenDone(channel);
            }
        }

        // The request line has its own limit in Netty, and long query parameters such as tokens count against it
        HttpDecoderConfig httpConfig = new HttpDecoderConfig()
            .setMaxInitialLineLength(configuration.getMaxHandshakeHeaderSize())
            .setMaxHeaderSize(configuration.getMaxHandshakeHeaderSize());

        pipeline.addLast(new HttpServerCodec(httpConfig));
        // An upgrade request needs no body, so a body is not allowed to take more memory than the headers
        pipeline.addLast(new HttpObjectAggregator(configuration.getMaxHandshakeHeaderSize()));

        if (configuration.isCompressionEnabled()) {
            // Negotiates permessage-deflate during the handshake, then puts the compression encoder and decoder
            // in its place, so the handlers below receive decompressed frames
            pipeline.addLast(new ContinuationRsvValidator());
            pipeline.addLast(newCompressionHandler(configuration));

            // The frame decoder lets reserved bits through once extensions are allowed, even if none was negotiated
            pipeline.addLast(new ReservedBitsValidator());
        }

        // Fails the connection with 1007 when a text message is not valid UTF-8 (RFC 6455, section 8.1).
        // It tracks the validation state across fragments, so it must see them before they are assembled.
        pipeline.addLast(new Utf8FrameValidator());

        // Assembles fragmented messages, so the handler only receives complete frames
        pipeline.addLast(new WebSocketFrameAggregator(configuration.getMaxMessageSize()) {
            @Override
            protected void handleOversizedMessage(ChannelHandlerContext context, WebSocketFrame oversized) {
                // Close with 1009 (message too big), like the frame decoder does for an oversized frame
                context.writeAndFlush(new CloseWebSocketFrame(WebSocketCloseStatus.MESSAGE_TOO_BIG))
                    .addListener(ChannelFutureListener.CLOSE);
            }
        });

        pipeline.addLast(new WebSocketServerHandler<>(webSocketServer));
    }

    /**
     * Counts the TLS handshake of the connection against the limit.
     * A TLS handshake costs far more CPU than the rest of the connection setup, so a reconnect storm past the limit
     * is turned away early instead of slowing down every handshake.
     *
     * @return false if the limit is reached.
     */
    private boolean acquireTlsHandshake(int maxConcurrentTlsHandshakes) {
        if (maxConcurrentTlsHandshakes == 0) {
            return true;
        }

        if (tlsHandshakes.incrementAndGet() > maxConcurrentTlsHandshakes) {
            tlsHandshakes.decrementAndGet();
            return false;
        }
        return true;
    }

    /**
     * Releases the counted TLS handshake once it completes or fails, or the connection closes before.
     * Added behind the TLS handler, which fires the completion event towards the handlers after it.
     */
    private void releaseTlsHandshakeWhenDone(Channel channel) {
        AtomicBoolean released = new AtomicBoolean();
        Runnable release = () -> {
            if (released.compareAndSet(false, true)) {
                tlsHandshakes.decrementAndGet();
            }
        };

        channel.closeFuture().addListener(_ -> release.run());
        channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void userEventTriggered(ChannelHandlerContext context, Object event) {
                context.fireUserEventTriggered(event);

                if (event instanceof SslHandshakeCompletionEvent) {
                    release.run();
                    context.pipeline().remove(this);
                }
            }
        });
    }

    private static SslContext createSslContext(TlsConfiguration configuration) {
        if (configuration == null) {
            return null;
        }

        if (configuration.sslContext() != null) {
            return new JdkSslContext(configuration.sslContext(), false, ClientAuth.NONE);
        }

        File certificateChain = configuration.certificateChain().toFile();
        File privateKey = configuration.privateKey().toFile();

        try {
            checkKeyMatchesCertificate(certificateChain, privateKey, configuration.privateKeyPassword());
            return SslContextBuilder.forServer(certificateChain, privateKey, configuration.privateKeyPassword()).build();
        } catch (SSLException | RuntimeException exception) {
            throw new IllegalArgumentException(
                "Could not create the server TLS context from " + certificateChain + " and " + privateKey + ": " + exception.getMessage(),
                exception
            );
        }
    }

    /**
     * Checks that the private key belongs to the first certificate of the chain by signing with the key and verifying
     * with the certificate. Netty accepts a mismatched pair, and every TLS handshake would fail later.
     *
     * @param certificateChain the certificate chain in PEM format.
     * @param privateKey       the private key in PEM format.
     * @param password         the password of the private key, or null.
     */
    private static void checkKeyMatchesCertificate(File certificateChain, File privateKey, String password) {
        X509Certificate certificate;
        PrivateKey key;

        try {
            certificate = PemFiles.certificates(certificateChain)[0];
            key = PemFiles.privateKey(privateKey, password);
        } catch (Exception exception) {
            throw new IllegalArgumentException("The certificate chain or private key cannot be read", exception);
        }

        String algorithm = switch (certificate.getPublicKey().getAlgorithm()) {
            case "RSA" -> "SHA256withRSA";
            case "EC" -> "SHA256withECDSA";
            case "Ed25519", "EdDSA" -> "Ed25519";
            default -> null;
        };

        // shortcut: other key types are not checked, add their signature algorithm when a user needs one
        if (algorithm == null) {
            return;
        }

        try {
            byte[] nonce = new byte[32];
            new SecureRandom().nextBytes(nonce);

            Signature signer = Signature.getInstance(algorithm);
            signer.initSign(key);
            signer.update(nonce);
            byte[] signature = signer.sign();

            Signature verifier = Signature.getInstance(algorithm);
            verifier.initVerify(certificate);
            verifier.update(nonce);

            if (verifier.verify(signature)) {
                return;
            }
        } catch (GeneralSecurityException exception) {
            throw new IllegalArgumentException("The private key does not match the certificate", exception);
        }

        throw new IllegalArgumentException("The private key does not match the certificate");
    }

    /**
     * Reads PEM files with the parsers Netty uses, which are only available to subclasses of SslContext.
     */
    private abstract static class PemFiles extends SslContext {

        static X509Certificate[] certificates(File file) throws CertificateException {
            return toX509Certificates(file);
        }

        static PrivateKey privateKey(File file, String password) throws Exception {
            return toPrivateKey(file, password);
        }
    }

    /**
     * Creates the handler negotiating permessage-deflate.
     * An offer with server_max_window_bits is accepted only for the full 15-bit window, the only one the JDK's zlib
     * compresses with.
     * A malformed offer, e.g. with a non-numeric client_max_window_bits, is declined (RFC 7692, section 7), so the
     * connection is upgraded without compression.
     * Offers spread over several Sec-WebSocket-Extensions headers are read as one (RFC 6455, section 9.1).
     *
     * With server_no_context_takeover configured, the server adds it to every offer, which RFC 7692 (section 7.1.1.1)
     * allows also when the client did not request it.
     *
     * @param configuration the server configuration.
     * @return the extension handler.
     */
    private static WebSocketServerExtensionHandler newCompressionHandler(WebSocketServerConfiguration<?> configuration) {
        PerMessageDeflateServerExtensionHandshaker withoutWindowBits = newDeflateHandshaker(false, configuration);
        PerMessageDeflateServerExtensionHandshaker withWindowBits = newDeflateHandshaker(true, configuration);
        String fullWindow = String.valueOf(PerMessageDeflateServerExtensionHandshaker.MAX_WINDOW_SIZE);
        boolean serverNoContextTakeover = configuration.isServerNoContextTakeover();

        return new WebSocketServerExtensionHandler(
            declineMalformed(offer -> withoutWindowBits.handshakeExtension(withServerParameters(offer, serverNoContextTakeover))),
            declineMalformed(offer -> fullWindow.equals(offer.parameters().get(SERVER_MAX_WINDOW_BITS))
                ? withWindowBits.handshakeExtension(withServerParameters(offer, serverNoContextTakeover))
                : null)
        ) {
            @Override
            protected void onHttpRequestChannelRead(ChannelHandlerContext context, HttpRequest request) throws Exception {
                List<String> offers = request.headers().getAll(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS);

                if (offers.size() > 1) {
                    request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS, String.join(", ", offers));
                }
                super.onHttpRequestChannelRead(context, request);
            }
        };
    }

    private static WebSocketExtensionData withServerParameters(WebSocketExtensionData offer, boolean serverNoContextTakeover) {
        if (!serverNoContextTakeover || offer.parameters().containsKey(SERVER_NO_CONTEXT_TAKEOVER)) {
            return offer;
        }

        Map<String, String> parameters = new LinkedHashMap<>(offer.parameters());
        parameters.put(SERVER_NO_CONTEXT_TAKEOVER, null);
        return new WebSocketExtensionData(offer.name(), parameters);
    }

    /**
     * Netty parses the parameters of an offer while negotiating, and its exception would skip the request.
     */
    private static WebSocketServerExtensionHandshaker declineMalformed(WebSocketServerExtensionHandshaker handshaker) {
        return offer -> {
            try {
                return handshaker.handshakeExtension(offer);
            } catch (RuntimeException exception) {
                return null;
            }
        };
    }

    /**
     * Creates the permessage-deflate handshaker with the configured level, accepting server_no_context_takeover.
     * Frames below the compression threshold are sent uncompressed.
     *
     * @param allowServerWindowSize whether to accept server_max_window_bits.
     * @param configuration         the server configuration.
     * @return the permessage-deflate handshaker.
     */
    private static PerMessageDeflateServerExtensionHandshaker newDeflateHandshaker(
        boolean allowServerWindowSize,
        WebSocketServerConfiguration<?> configuration
    ) {
        int threshold = configuration.getCompressionThreshold();
        WebSocketExtensionFilter belowThreshold = frame -> frame.content().readableBytes() < threshold;
        WebSocketExtensionFilterProvider filters = new WebSocketExtensionFilterProvider() {
            @Override
            public WebSocketExtensionFilter encoderFilter() {
                return belowThreshold;
            }

            @Override
            public WebSocketExtensionFilter decoderFilter() {
                return WebSocketExtensionFilter.NEVER_SKIP;
            }
        };

        return new PerMessageDeflateServerExtensionHandshaker(
            configuration.getCompressionLevel(),
            allowServerWindowSize,
            PerMessageDeflateServerExtensionHandshaker.MAX_WINDOW_SIZE,
            true,
            false,
            filters,
            configuration.getMaxMessageSize()
        );
    }
}
