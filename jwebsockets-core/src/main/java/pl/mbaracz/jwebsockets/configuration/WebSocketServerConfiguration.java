package pl.mbaracz.jwebsockets.configuration;

import pl.mbaracz.jwebsockets.TlsConfiguration;
import pl.mbaracz.jwebsockets.message.MessageDecoder;
import pl.mbaracz.jwebsockets.message.MessageEncoder;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.regex.Pattern;

/**
 * Immutable configuration of a WebSocket server, created with its {@link Builder}.
 *
 * @param <T> the type of WebSocket messages.
 */
public final class WebSocketServerConfiguration<T> {

    /**
     * Indicates whether text frames are allowed.
     * If false, a text message closes the connection with status 1003.
     */
    private final boolean allowTextFrames;

    /**
     * If true, responses will contain binary frame instead of text frame.
     */
    private final boolean respondWithBinaryFrame;

    /**
     * Indicates whether binary frames are allowed.
     * If false, a binary message closes the connection with status 1003.
     */
    private final boolean allowBinaryFrames;

    private final TlsConfiguration tlsConfiguration;

    /**
     * Indicates whether the connection should be closed on an exception.
     */
    private final boolean closeOnException;

    /**
     * Maximum total size of the HTTP headers in a WebSocket upgrade request.
     */
    private final int maxHandshakeHeaderSize;

    /**
     * Maximum payload size of a single WebSocket frame.
     */
    private final int maxFrameSize;

    /**
     * Maximum size of a complete WebSocket message, including all fragments.
     */
    private final int maxMessageSize;

    /**
     * Time a connection has to complete the WebSocket handshake before it is closed.
     */
    private final Duration handshakeTimeout;

    /**
     * Time without received data after which a session is pinged, null if heartbeat is disabled.
     */
    private final Duration heartbeatInterval;

    /**
     * Time to wait for the pong after a heartbeat ping before the session is closed.
     */
    private final Duration heartbeatTimeout;

    /**
     * Time without an application message after which a session is closed, null to disable the timeout.
     */
    private final Duration idleTimeout;

    /**
     * Time to wait for the client's close frame after the server sent its own before the connection is closed.
     */
    private final Duration closeTimeout;

    /**
     * Time a topic broker operation may take before it fails.
     */
    private final Duration brokerOperationTimeout;

    /**
     * Executor running the application callbacks, null to run them on the event loop.
     */
    private final Executor callbackExecutor;

    /**
     * Callbacks of a session that may wait for the callback executor before the server stops reading from the session.
     */
    private final int maxPendingCallbacks;

    /**
     * Low write buffer watermark in bytes, or null to use the Netty default.
     */
    private final Integer writeBufferLowWaterMark;

    /**
     * High write buffer watermark in bytes, or null to use the Netty default.
     */
    private final Integer writeBufferHighWaterMark;
    /**
     * Policy for new messages while a connection is unwritable.
     */
    private final BackpressurePolicy backpressurePolicy;

    /**
     * Time a connection may stay unwritable before it is closed, null to keep unwritable connections open.
     */
    private final Duration unwritableTimeout;

    /**
     * List of allowed origins.
     */
    private final List<String> allowedOrigins;

    /**
     * Pattern for allowed origins.
     */
    private final Pattern allowedOriginPattern;

    /**
     * Subprotocols the server supports, empty if subprotocols are not negotiated.
     */
    private final List<String> subprotocols;

    /**
     * Indicates whether the permessage-deflate compression is negotiated with clients that offer it.
     */
    private final boolean compressionEnabled;

    /**
     * Message encoder for encoding messages of type T.
     */
    private final MessageEncoder<T> messageEncoder;

    /**
     * Message decoder for decoding messages of type T.
     */
    private final MessageDecoder<T> messageDecoder;

    private WebSocketServerConfiguration(Builder<T> builder) {
        this.allowTextFrames = builder.allowTextFrames;
        this.respondWithBinaryFrame = builder.respondWithBinaryFrame;
        this.allowBinaryFrames = builder.allowBinaryFrames;
        this.tlsConfiguration = builder.tlsConfiguration;
        this.closeOnException = builder.closeOnException;
        this.maxHandshakeHeaderSize = builder.maxHandshakeHeaderSize;
        this.maxFrameSize = builder.maxFrameSize;
        this.maxMessageSize = builder.maxMessageSize;
        this.handshakeTimeout = builder.handshakeTimeout;
        this.heartbeatInterval = builder.heartbeatInterval;
        this.heartbeatTimeout = builder.heartbeatTimeout;
        this.idleTimeout = builder.idleTimeout;
        this.closeTimeout = builder.closeTimeout;
        this.brokerOperationTimeout = builder.brokerOperationTimeout;
        this.callbackExecutor = builder.callbackExecutor;
        this.maxPendingCallbacks = builder.maxPendingCallbacks;
        this.writeBufferLowWaterMark = builder.writeBufferLowWaterMark;
        this.writeBufferHighWaterMark = builder.writeBufferHighWaterMark;
        this.backpressurePolicy = builder.backpressurePolicy;
        this.unwritableTimeout = builder.unwritableTimeout;
        this.allowedOrigins = builder.allowedOrigins;
        this.allowedOriginPattern = builder.allowedOriginPattern;
        this.subprotocols = builder.subprotocols;
        this.compressionEnabled = builder.compressionEnabled;
        this.messageEncoder = builder.messageEncoder;
        this.messageDecoder = builder.messageDecoder;
    }

    /**
     * Creates a builder with the default settings.
     *
     * @param <T> the type of WebSocket messages.
     * @return The new builder.
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Creates a builder starting from the settings of this configuration.
     *
     * @return The new builder.
     */
    public Builder<T> toBuilder() {
        Builder<T> builder = new Builder<>();
        builder.allowTextFrames = allowTextFrames;
        builder.respondWithBinaryFrame = respondWithBinaryFrame;
        builder.allowBinaryFrames = allowBinaryFrames;
        builder.tlsConfiguration = tlsConfiguration;
        builder.closeOnException = closeOnException;
        builder.maxHandshakeHeaderSize = maxHandshakeHeaderSize;
        builder.maxFrameSize = maxFrameSize;
        builder.maxMessageSize = maxMessageSize;
        builder.handshakeTimeout = handshakeTimeout;
        builder.heartbeatInterval = heartbeatInterval;
        builder.heartbeatTimeout = heartbeatTimeout;
        builder.idleTimeout = idleTimeout;
        builder.closeTimeout = closeTimeout;
        builder.brokerOperationTimeout = brokerOperationTimeout;
        builder.callbackExecutor = callbackExecutor;
        builder.maxPendingCallbacks = maxPendingCallbacks;
        builder.writeBufferLowWaterMark = writeBufferLowWaterMark;
        builder.writeBufferHighWaterMark = writeBufferHighWaterMark;
        builder.backpressurePolicy = backpressurePolicy;
        builder.unwritableTimeout = unwritableTimeout;
        builder.allowedOrigins = allowedOrigins;
        builder.allowedOriginPattern = allowedOriginPattern;
        builder.subprotocols = subprotocols;
        builder.compressionEnabled = compressionEnabled;
        builder.messageEncoder = messageEncoder;
        builder.messageDecoder = messageDecoder;
        return builder;
    }

    public boolean isAllowTextFrames() {
        return allowTextFrames;
    }

    public boolean isRespondWithBinaryFrame() {
        return respondWithBinaryFrame;
    }

    public boolean isAllowBinaryFrames() {
        return allowBinaryFrames;
    }

    public TlsConfiguration getTlsConfiguration() {
        return tlsConfiguration;
    }

    public boolean isCloseOnException() {
        return closeOnException;
    }

    public int getMaxHandshakeHeaderSize() {
        return maxHandshakeHeaderSize;
    }

    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public Duration getHandshakeTimeout() {
        return handshakeTimeout;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public Duration getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public Duration getCloseTimeout() {
        return closeTimeout;
    }

    public Duration getBrokerOperationTimeout() {
        return brokerOperationTimeout;
    }

    public Executor getCallbackExecutor() {
        return callbackExecutor;
    }

    public int getMaxPendingCallbacks() {
        return maxPendingCallbacks;
    }

    public Integer getWriteBufferLowWaterMark() {
        return writeBufferLowWaterMark;
    }

    public Integer getWriteBufferHighWaterMark() {
        return writeBufferHighWaterMark;
    }

    public BackpressurePolicy getBackpressurePolicy() {
        return backpressurePolicy;
    }

    public Duration getUnwritableTimeout() {
        return unwritableTimeout;
    }

    public List<String> getAllowedOrigins() {
        return allowedOrigins;
    }

    public Pattern getAllowedOriginPattern() {
        return allowedOriginPattern;
    }

    public List<String> getSubprotocols() {
        return subprotocols;
    }

    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }

    public MessageEncoder<T> getMessageEncoder() {
        return messageEncoder;
    }

    public MessageDecoder<T> getMessageDecoder() {
        return messageDecoder;
    }

    /**
     * Builder of a {@link WebSocketServerConfiguration}, given to the configurer of {@code WebSocketServer.configure()}.
     *
     * @param <T> the type of WebSocket messages.
     */
    public static final class Builder<T> {

        // The event loops schedule timeouts in nanoseconds, a longer duration would overflow
        private static final Duration MAX_DURATION = Duration.ofNanos(Long.MAX_VALUE);

        private boolean allowTextFrames = true;
        private boolean respondWithBinaryFrame;
        private boolean allowBinaryFrames;
        private TlsConfiguration tlsConfiguration;
        private boolean closeOnException;
        private int maxHandshakeHeaderSize = 8 * 1024;
        private int maxFrameSize = 1024 * 1024;
        private int maxMessageSize = 1024 * 1024;
        private Duration handshakeTimeout = Duration.ofSeconds(10);
        private Duration heartbeatInterval;
        private Duration heartbeatTimeout = Duration.ofSeconds(10);
        private Duration idleTimeout;
        private Duration closeTimeout = Duration.ofSeconds(5);
        private Duration brokerOperationTimeout = Duration.ofSeconds(30);
        private Executor callbackExecutor;
        private int maxPendingCallbacks = 64;
        private Integer writeBufferLowWaterMark;
        private Integer writeBufferHighWaterMark;
        private BackpressurePolicy backpressurePolicy = BackpressurePolicy.BUFFER;
        private Duration unwritableTimeout;
        private List<String> allowedOrigins;
        private Pattern allowedOriginPattern;
        private List<String> subprotocols = List.of();
        private boolean compressionEnabled;
        private MessageEncoder<T> messageEncoder;
        private MessageDecoder<T> messageDecoder;

        private Builder() {
        }

        /**
         * Sets the server-side TLS configuration used for new connections. Pass null to disable TLS.
         *
         * @param tlsConfiguration Server-side TLS configuration, or null to disable TLS.
         * @return This builder.
         */
        public Builder<T> setTlsConfiguration(TlsConfiguration tlsConfiguration) {
            this.tlsConfiguration = tlsConfiguration;
            return this;
        }

        /**
         * Sets whether the connection should be closed on an exception.
         * An open session is closed with 1007 when its message cannot be decoded and with 1011 on other server-side failures.
         *
         * @param closeOnException True to close the connection on an exception, false to keep it open (default).
         * @return This builder.
         */
        public Builder<T> setCloseOnException(boolean closeOnException) {
            this.closeOnException = closeOnException;
            return this;
        }

        /**
         * Sets the maximum total size of the HTTP headers in a WebSocket upgrade request.
         * The request line, including the query string, and a request body each have the same limit.
         * A larger request is rejected before the WebSocket handshake.
         *
         * @param maxHandshakeHeaderSize Maximum total header size in bytes, must be positive.
         * @return This builder.
         */
        public Builder<T> setMaxHandshakeHeaderSize(int maxHandshakeHeaderSize) {
            if (maxHandshakeHeaderSize <= 0) {
                throw new IllegalArgumentException("Maximum handshake header size must be positive!");
            }
            this.maxHandshakeHeaderSize = maxHandshakeHeaderSize;
            return this;
        }

        /**
         * Sets the maximum payload size of a single WebSocket frame.
         * A larger frame closes the connection with status 1009 (message too big).
         * With compression, it limits the compressed payload, and a decompressed frame is limited by the maximum message size.
         * The configured frame size must not exceed the maximum message size when the configuration is built.
         *
         * @param maxFrameSize Maximum frame payload size in bytes, must be positive.
         * @return This builder.
         */
        public Builder<T> setMaxFrameSize(int maxFrameSize) {
            if (maxFrameSize <= 0) {
                throw new IllegalArgumentException("Maximum frame size must be positive!");
            }
            this.maxFrameSize = maxFrameSize;
            return this;
        }

        /**
         * Sets the maximum size of a complete WebSocket message, including all fragments.
         * A larger message closes the connection with status 1009 (message too big).
         * The configured message size must not be smaller than the maximum frame size when the configuration is built.
         *
         * @param maxMessageSize Maximum message size in bytes, must be positive.
         * @return This builder.
         */
        public Builder<T> setMaxMessageSize(int maxMessageSize) {
            if (maxMessageSize <= 0) {
                throw new IllegalArgumentException("Maximum message size must be positive!");
            }
            this.maxMessageSize = maxMessageSize;
            return this;
        }

        /**
         * Sets how long a connection may take to complete the WebSocket handshake,
         * including the TLS handshake, before it is closed.
         *
         * @param handshakeTimeout Handshake timeout, positive, 10 seconds by default.
         * @return This builder.
         */
        public Builder<T> setHandshakeTimeout(Duration handshakeTimeout) {
            if (handshakeTimeout == null || !handshakeTimeout.isPositive()) {
                throw new IllegalArgumentException("Handshake timeout must be positive!");
            }
            requireSchedulable(handshakeTimeout, "Handshake timeout");
            this.handshakeTimeout = handshakeTimeout;
            return this;
        }

        /**
         * Sets the heartbeat interval. A session that receives nothing for this long is sent a ping.
         * No ping is sent while the session's write buffer is full, see {@link #setUnwritableTimeout(Duration)}.
         *
         * @param heartbeatInterval Heartbeat interval, positive, or null to disable heartbeat (default).
         * @return This builder.
         */
        public Builder<T> setHeartbeatInterval(Duration heartbeatInterval) {
            if (heartbeatInterval != null && !heartbeatInterval.isPositive()) {
                throw new IllegalArgumentException("Heartbeat interval must be positive!");
            }
            requireSchedulable(heartbeatInterval, "Heartbeat interval");
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        /**
         * Sets how long to wait for the pong after a heartbeat ping before the session is closed.
         *
         * @param heartbeatTimeout Heartbeat timeout, positive, 10 seconds by default.
         * @return This builder.
         */
        public Builder<T> setHeartbeatTimeout(Duration heartbeatTimeout) {
            if (heartbeatTimeout == null || !heartbeatTimeout.isPositive()) {
                throw new IllegalArgumentException("Heartbeat timeout must be positive!");
            }
            requireSchedulable(heartbeatTimeout, "Heartbeat timeout");
            this.heartbeatTimeout = heartbeatTimeout;
            return this;
        }

        /**
         * Sets how long a session may receive no text or binary message before it is closed.
         * Control frames such as ping and pong do not reset this timeout.
         *
         * @param idleTimeout Idle timeout, positive, or null to disable it (default).
         * @return This builder.
         */
        public Builder<T> setIdleTimeout(Duration idleTimeout) {
            if (idleTimeout != null && !idleTimeout.isPositive()) {
                throw new IllegalArgumentException("Idle timeout must be positive!");
            }
            requireSchedulable(idleTimeout, "Idle timeout");
            this.idleTimeout = idleTimeout;
            return this;
        }

        /**
         * Sets how long to wait for the client's close frame after the server sent its own before the connection is closed.
         *
         * @param closeTimeout Close timeout, positive, 5 seconds by default.
         * @return This builder.
         */
        public Builder<T> setCloseTimeout(Duration closeTimeout) {
            if (closeTimeout == null || !closeTimeout.isPositive()) {
                throw new IllegalArgumentException("Close timeout must be positive!");
            }
            requireSchedulable(closeTimeout, "Close timeout");
            this.closeTimeout = closeTimeout;
            return this;
        }

        /**
         * Sets how long a topic broker operation (subscribe, unsubscribe or publish) may take before it fails, so a broker
         * that does not answer does not block the later operations of the topic.
         *
         * @param brokerOperationTimeout Broker operation timeout, positive, 30 seconds by default.
         * @return This builder.
         */
        public Builder<T> setBrokerOperationTimeout(Duration brokerOperationTimeout) {
            if (brokerOperationTimeout == null || !brokerOperationTimeout.isPositive()) {
                throw new IllegalArgumentException("Broker operation timeout must be positive!");
            }
            requireSchedulable(brokerOperationTimeout, "Broker operation timeout");
            this.brokerOperationTimeout = brokerOperationTimeout;
            return this;
        }

        /**
         * Sets the executor for the onOpen, onMessage, onWritabilityChanged and onClose callbacks, so they can block
         * without stopping the event loop. The callbacks of one session still run one at a time and in order.
         * The server never shuts the executor down, its lifecycle belongs to the application.
         * A session whose callback the executor rejects is closed with 1013, and a rejected onClose callback runs on the event loop instead.
         * An executor that runs tasks on the submitting thread, e.g. with CallerRunsPolicy, can run callbacks on the event loop.
         *
         * @param callbackExecutor Callback executor, or null to run the callbacks on the event loop (default).
         * @return This builder.
         */
        public Builder<T> setCallbackExecutor(Executor callbackExecutor) {
            this.callbackExecutor = callbackExecutor;
            return this;
        }

        /**
         * Sets how many callbacks of a session may wait for the callback executor before the server stops reading
         * from the session. Reading resumes once half of them have run, so a slow callback slows down the client.
         *
         * @param maxPendingCallbacks Maximum number of pending callbacks per session, positive, 64 by default.
         * @return This builder.
         */
        public Builder<T> setMaxPendingCallbacks(int maxPendingCallbacks) {
            if (maxPendingCallbacks <= 0) {
                throw new IllegalArgumentException("Max pending callbacks must be positive!");
            }
            this.maxPendingCallbacks = maxPendingCallbacks;
            return this;
        }

        /**
         * Sets the write buffer watermarks of the connections. A connection becomes unwritable when more than the high
         * watermark waits to be written, and writable again once less than the low watermark is left.
         *
         * @param low  Low watermark in bytes.
         * @param high High watermark in bytes, at least the low watermark.
         * @return This builder.
         */
        public Builder<T> setWriteBufferWaterMark(int low, int high) {
            if (low < 0) {
                throw new IllegalArgumentException("Low write buffer watermark must not be negative!");
            }
            if (high < low) {
                throw new IllegalArgumentException("High write buffer watermark must not be lower than the low watermark!");
            }
            this.writeBufferLowWaterMark = low;
            this.writeBufferHighWaterMark = high;
            return this;
        }

        /**
         * Sets what happens to new messages while a connection is unwritable.
         * {@link BackpressurePolicy#BUFFER} keeps accepting them into Netty's outbound buffer and is the default.
         * {@link BackpressurePolicy#REJECT_NEW} rejects them until the connection becomes writable again.
         * In both cases, a configured unwritable timeout still closes a connection that remains unwritable.
         *
         * @param backpressurePolicy Backpressure policy, not null.
         * @return This builder.
         */
        public Builder<T> setBackpressurePolicy(BackpressurePolicy backpressurePolicy) {
            this.backpressurePolicy = Objects.requireNonNull(backpressurePolicy, "Backpressure policy must not be null!");
            return this;
        }

        /**
         * Sets how long the write buffer of an unwritable connection may not drain, for example because the client stopped
         * reading, before the connection is closed. A client that reads a large message slowly keeps its connection.
         *
         * @param unwritableTimeout Unwritable timeout, positive, or null to keep unwritable connections open (default).
         * @return This builder.
         */
        public Builder<T> setUnwritableTimeout(Duration unwritableTimeout) {
            if (unwritableTimeout != null && !unwritableTimeout.isPositive()) {
                throw new IllegalArgumentException("Unwritable timeout must be positive!");
            }
            requireSchedulable(unwritableTimeout, "Unwritable timeout");
            this.unwritableTimeout = unwritableTimeout;
            return this;
        }

        /**
         * Sets the allowed origins.
         *
         * @param origins Allowed origins.
         * @return This builder.
         */
        public Builder<T> setAllowedOrigins(String... origins) {
            this.allowedOrigins = List.of(origins);
            return this;
        }

        /**
         * Sets the allowed origins.
         *
         * @param origins List of allowed origins.
         * @return This builder.
         */
        public Builder<T> setAllowedOrigins(List<String> origins) {
            this.allowedOrigins = origins == null ? null : List.copyOf(origins);
            return this;
        }

        /**
         * Sets the supported subprotocols.
         * The handshake selects the first subprotocol requested by the client that is also supported.
         *
         * @param subprotocols Supported subprotocols.
         * @return This builder.
         */
        public Builder<T> setSubprotocols(String... subprotocols) {
            this.subprotocols = List.of(subprotocols);
            return this;
        }

        /**
         * Sets whether to negotiate the permessage-deflate compression (RFC 7692) with clients that offer it.
         *
         * @param compressionEnabled True to compress messages of clients that support it, false otherwise (default).
         * @return This builder.
         */
        public Builder<T> setCompressionEnabled(boolean compressionEnabled) {
            this.compressionEnabled = compressionEnabled;
            return this;
        }

        /**
         * Sets the allowed origin pattern.
         *
         * @param pattern Pattern for allowed origins.
         * @return This builder.
         */
        public Builder<T> setAllowedOriginPattern(Pattern pattern) {
            this.allowedOriginPattern = pattern;
            return this;
        }

        /**
         * Sets the message encoder.
         *
         * @param encoder The message encoder.
         * @return This builder.
         */
        public Builder<T> setMessageEncoder(MessageEncoder<T> encoder) {
            this.messageEncoder = encoder;
            return this;
        }

        /**
         * Sets the message decoder.
         *
         * @param decoder The message decoder.
         * @return This builder.
         */
        public Builder<T> setMessageDecoder(MessageDecoder<T> decoder) {
            this.messageDecoder = decoder;
            return this;
        }

        /**
         * Sets whether text frames are allowed.
         *
         * @param allowTextFrames True to allow text frames, false otherwise.
         * @return This builder.
         */
        public Builder<T> setAllowTextFrames(boolean allowTextFrames) {
            this.allowTextFrames = allowTextFrames;
            return this;
        }

        /**
         * Sets whether binary frames are allowed.
         *
         * @param allowBinaryFrames True to allow binary frames, false otherwise.
         * @return This builder.
         */
        public Builder<T> setAllowBinaryFrames(boolean allowBinaryFrames) {
            this.allowBinaryFrames = allowBinaryFrames;
            return this;
        }

        /**
         * Sets whether to respond with binary frames.
         *
         * @param respondWithBinaryFrame True to respond with binary frames, false otherwise.
         * @return This builder.
         */
        public Builder<T> setRespondWithBinaryFrame(boolean respondWithBinaryFrame) {
            this.respondWithBinaryFrame = respondWithBinaryFrame;
            return this;
        }

        /**
         * Builds the immutable configuration.
         *
         * @return The new WebSocketServerConfiguration instance.
         * @throws IllegalArgumentException if the maximum frame size exceeds the maximum message size.
         */
        private static void requireSchedulable(Duration duration, String name) {
            if (duration != null && duration.compareTo(MAX_DURATION) > 0) {
                throw new IllegalArgumentException(name + " must not exceed 292 years!");
            }
        }

        public WebSocketServerConfiguration<T> build() {
            if (maxFrameSize > maxMessageSize) {
                throw new IllegalArgumentException("Maximum frame size must not exceed maximum message size!");
            }
            return new WebSocketServerConfiguration<>(this);
        }
    }
}
