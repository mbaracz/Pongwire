package pl.mbaracz.pongwire;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.HttpDecoderConfig;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
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
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import pl.mbaracz.pongwire.configuration.WebSocketServerConfiguration;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import java.io.File;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.interfaces.EdECKey;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Channel initializer for setting up the pipeline for WebSocket server channels.
 *
 * @param <T> the type of WebSocket messages.
 * @param <D> the type of additional data associated with WebSocket sessions.
 */
class WebSocketServerChannelInitializer<T, D> extends ChannelInitializer<Channel> {

    private static final String SERVER_MAX_WINDOW_BITS = "server_max_window_bits";
    private static final String SERVER_NO_CONTEXT_TAKEOVER = "server_no_context_takeover";
    private static final Pattern WINDOW_BITS = Pattern.compile("8|9|1[0-5]");

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
            int maxConcurrentTlsHandshakes = configuration.getMaxConcurrentTlsHandshakes();

            if (!acquireTlsHandshake(maxConcurrentTlsHandshakes)) {
                channel.close();
                return;
            }

            // Registered before the engine is created, so a failing setup, which closes the connection, releases the slot
            Runnable release = maxConcurrentTlsHandshakes > 0 ? releaseTlsHandshakeOnce() : () -> {};
            channel.closeFuture().addListener(_ -> release.run());

            SslHandler sslHandler = sslContext.newHandler(channel.alloc());
            // The server's handshake timeout runs from the accept and covers the TLS handshake too, so Netty's fixed
            // 10 seconds would only close connections early during a reconnect storm
            sslHandler.setHandshakeTimeoutMillis(0);
            if (sslParameters != null) {
                sslHandler.engine().setSSLParameters(sslParameters);
            }
            pipeline.addLast(sslHandler);

            if (maxConcurrentTlsHandshakes > 0) {
                pipeline.addLast(releaseWhenHandshakeCompletes(release));
            }
        }

        // The request line has its own limit in Netty, and long query parameters such as tokens count against it
        HttpDecoderConfig httpConfig = new HttpDecoderConfig()
            .setMaxInitialLineLength(configuration.getMaxHandshakeHeaderSize())
            .setMaxHeaderSize(configuration.getMaxHandshakeHeaderSize());

        // A separate decoder, so Netty replaces it with the WebSocket decoder as soon as the handshake starts.
        // The bytes a client sends in the same read as its upgrade request then leave the HTTP decoder as bytes
        // instead of being decoded as a broken HTTP request while the 101 response is still being written.
        // The encoder stays until the response is written.
        pipeline.addLast(new HttpRequestDecoder(httpConfig));
        pipeline.addLast(new HttpResponseEncoder());
        // An upgrade request needs no body, so a body is not allowed to take more memory than the headers
        pipeline.addLast(new HttpObjectAggregator(configuration.getMaxHandshakeHeaderSize()) {
            @Override
            protected void handleOversizedMessage(ChannelHandlerContext context, HttpMessage oversized) throws Exception {
                // Answered with 413 here, so the handler never sees the request and reports it to the observer
                WebSocketServerObserver<T, D> observer = webSocketServer.getObserver();

                if (observer != null) {
                    observer.handshakeRejected(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE.code());
                }
                super.handleOversizedMessage(context, oversized);
            }
        });

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
     * Returns the release of one counted TLS handshake, which decrements the count the first time it runs.
     */
    private Runnable releaseTlsHandshakeOnce() {
        AtomicBoolean released = new AtomicBoolean();
        return () -> {
            if (released.compareAndSet(false, true)) {
                tlsHandshakes.decrementAndGet();
            }
        };
    }

    /**
     * Releases the counted TLS handshake once it completes or fails.
     * Added behind the TLS handler, which fires the completion event towards the handlers after it.
     */
    private static ChannelInboundHandlerAdapter releaseWhenHandshakeCompletes(Runnable release) {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void userEventTriggered(ChannelHandlerContext context, Object event) {
                context.fireUserEventTriggered(event);

                if (event instanceof SslHandshakeCompletionEvent) {
                    release.run();
                    context.pipeline().remove(this);
                }
            }
        };
    }

    private static SslContext createSslContext(TlsConfiguration configuration) {
        if (configuration == null) {
            return null;
        }

        if (configuration.sslContext() != null) {
            // Netty would enable its own cipher suites and protocols, so the context's server engine says which to keep
            SSLEngine defaults = configuration.sslContext().createSSLEngine();
            defaults.setUseClientMode(false);

            return new JdkSslContext(
                configuration.sslContext(),
                false,
                List.of(defaults.getEnabledCipherSuites()),
                IdentityCipherSuiteFilter.INSTANCE,
                null,
                ClientAuth.NONE,
                defaults.getEnabledProtocols(),
                false
            );
        }

        File certificateChain = configuration.certificateChain().toFile();
        File privateKey = configuration.privateKey().toFile();
        X509Certificate[] chain;
        PrivateKey key;

        // Read once and handed to Netty as objects, so a rotation between two reads cannot pair the certificate
        // of one version with the key of another
        try {
            chain = PemFiles.certificates(certificateChain);
            key = PemFiles.privateKey(privateKey, configuration.privateKeyPassword());
        } catch (Exception exception) {
            throw new IllegalArgumentException(
                "The certificate chain " + certificateChain + " or private key " + privateKey + " cannot be read: " + exception.getMessage(),
                exception
            );
        }

        try {
            checkKeyMatchesCertificate(chain[0], key);
            return SslContextBuilder.forServer(key, chain).build();
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
     * @param certificate the first certificate of the chain.
     * @param key         the private key.
     */
    private static void checkKeyMatchesCertificate(X509Certificate certificate, PrivateKey key) {
        String algorithm = signatureAlgorithm(certificate.getPublicKey());

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
     * Returns the signature algorithm to check a key pair with, or null for a key type that is not checked.
     * The JDK reports both Edwards curves as EdDSA, the curve tells them apart.
     */
    static String signatureAlgorithm(PublicKey publicKey) {
        return switch (publicKey.getAlgorithm()) {
            case "RSA" -> "SHA256withRSA";
            case "EC" -> "SHA256withECDSA";
            case "Ed25519", "Ed448", "EdDSA" -> publicKey instanceof EdECKey edwardsKey ? edwardsKey.getParams().getName() : null;
            default -> null;
        };
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

                if (!offers.isEmpty()) {
                    request.headers().remove(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS);

                    try {
                        String wellFormed = wellFormedOffers(String.join(", ", offers));

                        if (wellFormed != null) {
                            request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS, wellFormed);
                        }
                    } catch (IllegalArgumentException exception) {
                        // The server handler answers a request that failed decoding with 400
                        request.setDecoderResult(DecoderResult.failure(exception));
                    }
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
     * Keeps only the offers Netty parses correctly.
     * Netty fails on an offer without a name, e.g. a header holding only a semicolon, before any handshaker sees it.
     * It also keeps the last of repeated parameters, accepts a value on the no-context-takeover flags
     * and parses window bits with a sign, all of which RFC 7692 (section 7) tells the server to decline.
     *
     * @param header the joined Sec-WebSocket-Extensions header.
     * @return the well-formed offers, or null if there is none.
     * @throws IllegalArgumentException if the header violates the grammar of RFC 6455 (section 9.1), e.g. an offer
     *                                  without a name, an empty parameter or an unbalanced quote.
     */
    static String wellFormedOffers(String header) {
        List<String> offers = new ArrayList<>();

        // Empty list elements are ignored like in any HTTP list (RFC 9110, section 5.6.1)
        for (String offer : header.split(",")) {
            if (!offer.isBlank() && hasValidParameters(offer)) {
                offers.add(offer.trim());
            }
        }
        return offers.isEmpty() ? null : String.join(", ", offers);
    }

    private static boolean hasValidParameters(String offer) {
        String[] parts = offer.split(";", -1);

        if (parts[0].isBlank()) {
            throw new IllegalArgumentException("Extension offer without a name: " + offer);
        }

        Set<String> names = new HashSet<>();

        for (int i = 1; i < parts.length; i++) {
            String[] parameter = parts[i].trim().split("=", 2);
            String name = parameter[0].trim().toLowerCase(Locale.ROOT);
            String value = parameter.length == 1 ? null : unquote(parameter[1].trim());

            if (name.isEmpty() || value != null && value.isEmpty()) {
                throw new IllegalArgumentException("Malformed extension parameter: " + parts[i]);
            }
            if (!names.add(name)) {
                return false;
            }
            if (name.endsWith("_no_context_takeover") && value != null) {
                return false;
            }
            if (name.endsWith("_max_window_bits") && (value != null || name.startsWith("server")) && !WINDOW_BITS.matcher(String.valueOf(value)).matches()) {
                return false;
            }
        }
        return true;
    }

    private static String unquote(String value) {
        boolean quoted = value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"");
        String content = quoted ? value.substring(1, value.length() - 1) : value;

        if (content.indexOf('"') >= 0) {
            throw new IllegalArgumentException("Malformed extension parameter value: " + value);
        }
        return content;
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
