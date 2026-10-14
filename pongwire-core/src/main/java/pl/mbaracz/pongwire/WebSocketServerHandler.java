package pl.mbaracz.pongwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.mbaracz.pongwire.configuration.BackpressurePolicy;
import pl.mbaracz.pongwire.configuration.WebSocketServerConfiguration;
import pl.mbaracz.pongwire.handler.CloseHandler;
import pl.mbaracz.pongwire.handler.ErrorHandler;
import pl.mbaracz.pongwire.handler.MessageHandler;
import pl.mbaracz.pongwire.handler.OpenHandler;
import pl.mbaracz.pongwire.handler.UpgradeHandler;
import pl.mbaracz.pongwire.handler.UpgradeResult;
import pl.mbaracz.pongwire.handler.WritabilityHandler;
import pl.mbaracz.pongwire.message.MessageDecodeException;
import pl.mbaracz.pongwire.message.MessageDecoder;

import javax.net.ssl.SSLSession;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

/**
 * Handles WebSocket and HTTP communication for the WebSocket server.
 *
 * @param <T> the type of WebSocket messages.
 * @param <D> the type of additional data associated with WebSocket sessions.
 */
final class WebSocketServerHandler<T, D> extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServerHandler.class);

    // A host name, an IPv4 address or a bracketed IPv6 address, with an optional port (RFC 3986, section 3.2.2)
    private static final Pattern HOST = Pattern.compile("(\\[[0-9A-Fa-f:.]+]|[A-Za-z0-9._~%!$&'()*+,;=-]+)(:[0-9]*)?");

    private final BiFunction<Supplier<byte[]>, ChannelHandlerContext, ChannelFuture> messageSender;
    private final WebSocketServer<T, D> webSocketServer;
    private final ErrorHandler<T, D> errorHandler;
    private final WebSocketServerObserver<T, D> observer;
    private WebSocketServerHandshaker handshaker;

    // Runs the application callbacks of this connection one at a time and in order,
    // null to run them on the event loop
    private final SerialExecutor callbackExecutor;

    // Callbacks submitted to the callback executor that have not finished yet.
    // Reading from the connection stops at the configured maximum and resumes
    // at half of it, both decided on the event loop.
    private final AtomicInteger pendingCallbacks = new AtomicInteger();

    // The session opened on this connection and whether its closure was already handled,
    // so a close frame and the following channelInactive() report it only once
    private WebSocketSession<T, D> openedSession;
    private boolean closeHandled;

    // The close callback of the session, the only one that still runs when the callback executor rejects it,
    // and whether a rejection was already reported, so a rejected backlog is reported once
    private volatile Runnable closeCallback;
    private boolean rejectionReported;

    // Whether a writability callback waits to run, and the writability it reported last, accessed only by the callbacks
    // and set to the writability at the opening of the session before the first one is submitted
    private final AtomicBoolean writabilityCallbackPending = new AtomicBoolean();
    private boolean reportedWritable;

    // Payload of the most recent Ping received while the channel was unwritable, answered once it is writable again
    private ByteBuf deferredPongPayload;

    // Frames the client sent before it got the 101 response, e.g. in the same read as its upgrade request,
    // handled once the session is opened
    private List<WebSocketFrame> framesBeforeOpen;

    // Pending close of a connection that has not completed the WebSocket handshake
    private ScheduledFuture<?> handshakeTimeout;

    // Pending close of the connection after a heartbeat ping, cancelled only by a pong
    private ScheduledFuture<?> heartbeatTimeout;

    // Pending close when no application message is received for the configured time
    private ScheduledFuture<?> idleTimeout;
    private long lastActivityNanos;

    // Pending close of a connection whose write buffer does not drain, cancelled when it becomes writable again,
    // and the pending bytes when it was scheduled, so a slowly draining buffer starts a new timeout
    private ScheduledFuture<?> unwritableTimeout;
    private long unwritablePendingBytes;

    /**
     * Constructs a WebSocketServerHandler with the provided WebSocket server.
     *
     * @param webSocketServer the WebSocket server instance.
     */
    WebSocketServerHandler(WebSocketServer<T, D> webSocketServer) {
        this.webSocketServer = webSocketServer;
        this.errorHandler = webSocketServer.getErrorHandler();
        this.observer = webSocketServer.getObserver();
        this.messageSender = sendOnEventLoop(
            rejectMessagesWhileClosing(
                rejectMessagesUnderBackpressure(
                    observeSends(getMessageSender(webSocketServer.getConfiguration())),
                    webSocketServer.getConfiguration().getBackpressurePolicy()
                )
            )
        );

        Executor executor = webSocketServer.getConfiguration().getCallbackExecutor();
        this.callbackExecutor = executor == null ? null : new SerialExecutor(executor);
    }

    /**
     * Determines the message sender based on the WebSocket server configuration.
     *
     * @param configuration the WebSocket server configuration.
     * @return the message sender function, returning the future of the write.
     */
    private BiFunction<Supplier<byte[]>, ChannelHandlerContext, ChannelFuture> getMessageSender(WebSocketServerConfiguration<T> configuration) {
        // The payload is encoded once, so a broadcast can wrap the same array for every session
        if (configuration.isRespondWithBinaryFrame()) {
            return (payload, context) -> context.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(payload.get())));
        }
        return (payload, context) -> context.writeAndFlush(new TextWebSocketFrame(Unpooled.wrappedBuffer(payload.get())));
    }

    /**
     * Reports encoder and write failures through the same channel exception path as inbound failures,
     * and notifies the observer about every message written.
     * Encoder exceptions are converted to failed futures so asynchronous callers always receive
     * them through their stage.
     *
     * @param messageSender the message sender.
     * @return the message sender observing its writes.
     */
    private BiFunction<Supplier<byte[]>, ChannelHandlerContext, ChannelFuture> observeSends(
        BiFunction<Supplier<byte[]>, ChannelHandlerContext, ChannelFuture> messageSender
    ) {
        return (message, context) -> {
            ChannelFuture write;

            try {
                write = messageSender.apply(message, context);
            } catch (RuntimeException exception) {
                // The encoder failed, a server failure whatever the type of the exception
                if (!closeHandled && context.channel().isActive()) {
                    handleCallbackFailure(context, exception);
                }
                return context.channel().newFailedFuture(exception);
            }

            write.addListener(result -> {
                if (result.isSuccess()) {
                    // The listener runs on the event loop, where the session was opened before it could send anything
                    if (observer != null) {
                        notifyObserver(observer -> observer.messageSent(openedSession));
                    }
                } else if (!closeHandled && context.channel().isActive()) {
                    // A send racing the closure of the session fails only the caller's stage, the session was already reported closed
                    handleFailure(context, result.cause());
                }
            });

            return write;
        };
    }

    /**
     * Runs the closing check and the write in one event-loop task, ordered with inbound Close handling.
     */
    private BiFunction<Supplier<byte[]>, ChannelHandlerContext, ChannelFuture> sendOnEventLoop(
        BiFunction<Supplier<byte[]>, ChannelHandlerContext, ChannelFuture> messageSender
    ) {
        return (message, context) -> {
            Channel channel = context.channel();
            if (channel.eventLoop().inEventLoop()) {
                return messageSender.apply(message, context);
            }

            ChannelPromise result = channel.newPromise();
            try {
                channel.eventLoop().execute(() -> {
                    try {
                        messageSender.apply(message, context).addListener(write -> {
                            if (write.isSuccess()) {
                                result.trySuccess();
                            } else {
                                result.tryFailure(write.cause());
                            }
                        });
                    } catch (RuntimeException exception) {
                        result.tryFailure(exception);
                    }
                });
            } catch (RuntimeException exception) {
                result.tryFailure(exception);
            }
            return result;
        };
    }

    /**
     * Fails messages sent after either peer started the closing handshake (RFC 6455, section 5.5.1).
     *
     * @param messageSender the message sender.
     * @return the message sender rejecting messages while the session is closing.
     */
    private BiFunction<Supplier<byte[]>, ChannelHandlerContext, ChannelFuture> rejectMessagesWhileClosing(
        BiFunction<Supplier<byte[]>, ChannelHandlerContext, ChannelFuture> messageSender
    ) {
        return (message, context) -> {
            if (ClosingHandshake.isClosing(context.channel())) {
                return context.channel().newFailedFuture(new IllegalStateException("WebSocket session is closing"));
            }

            return messageSender.apply(message, context);
        };
    }

    /**
     * Rejects new messages while an active connection is unwritable when configured to do so.
     * Closed connections are left to the regular write path so callers receive the underlying channel failure.
     *
     * @param messageSender      the message sender.
     * @param backpressurePolicy the configured backpressure policy.
     * @return the sender applying the configured policy.
     */
    private BiFunction<Supplier<byte[]>, ChannelHandlerContext, ChannelFuture> rejectMessagesUnderBackpressure(
        BiFunction<Supplier<byte[]>, ChannelHandlerContext, ChannelFuture> messageSender,
        BackpressurePolicy backpressurePolicy
    ) {
        if (backpressurePolicy != BackpressurePolicy.REJECT_NEW) {
            return messageSender;
        }

        return (message, context) -> {
            Channel channel = context.channel();

            if (channel.isActive() && !channel.isWritable()) {
                return channel.newFailedFuture(new BackpressureException());
            }

            return messageSender.apply(message, context);
        };
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, Object object) {
        if (object instanceof FullHttpRequest) {
            LOGGER.debug("Received FullHttpRequest from channel with id {}", context.channel().id());
            handleHttpRequest(context, (FullHttpRequest) object);
        } else if (object instanceof WebSocketFrame frame && handshaker != null && openedSession == null) {
            // Netty replaces the HTTP decoder with the WebSocket decoder as soon as the handshake starts,
            // so frames may arrive while the 101 response is still being written
            if (framesBeforeOpen == null) {
                framesBeforeOpen = new ArrayList<>();
            }
            framesBeforeOpen.add(frame.retain());
        } else if (object instanceof WebSocketFrame) {
            handleWebSocketFrame(context, (WebSocketFrame) object);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext context) {
        context.flush();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext context) {
        // A deadline rather than a read timeout, so a client sending its request slowly cannot keep the connection open
        Duration timeout = webSocketServer.getConfiguration().getHandshakeTimeout();

        handshakeTimeout = context.executor().schedule(() -> {
            LOGGER.debug("Closing channel with id {}, the handshake did not complete in time", context.channel().id());
            context.close();
        }, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) {
        ChannelId channelId = context.channel().id();
        LOGGER.debug("Channel with id {} is now inactive", channelId);

        cancelHandshakeTimeout();
        cancelHeartbeatTimeout();
        cancelIdleTimeout();
        cancelUnwritableTimeout();
        releaseDeferredPong();

        if (framesBeforeOpen != null) {
            framesBeforeOpen.forEach(WebSocketFrame::release);
            framesBeforeOpen = null;
        }

        if (openedSession != null) {
            // Use the status the server closed the connection with, otherwise no close frame
            // was exchanged and the closure is abnormal (RFC 6455, section 7.1.5)
            CloseInfo closeInfo = context.channel().attr(CloseInfo.KEY).get();

            if (closeInfo == null) {
                closeInfo = CloseInfo.of(WebSocketCloseStatus.ABNORMAL_CLOSURE);
            }

            handleSessionClosed(context, openedSession, closeInfo);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext context) {
        // A recovery cancels the pending close first, so the Pong below filling the buffer again starts a new one
        if (openedSession != null && !closeHandled) {
            updateUnwritableTimeout(context.channel());
        }

        // Before the application callback, which could fill the write buffer again and keep the Pong waiting.
        // The field is cleared first, because the write can change the writability and call this method again.
        if (deferredPongPayload != null && !ClosingHandshake.answersPings(context.channel())) {
            releaseDeferredPong();
        } else if (deferredPongPayload != null && context.channel().isWritable()) {
            ByteBuf payload = deferredPongPayload;
            deferredPongPayload = null;
            context.writeAndFlush(new PongWebSocketFrame(payload));
        }

        WritabilityHandler<T, D> writabilityHandler = webSocketServer.getWritabilityHandler();

        // One pending callback reports the writability at the time it runs,
        // so changes while the callback executor is busy do not queue up.
        // A value already reported is not reported again.
        if (openedSession != null && !closeHandled && writabilityHandler != null && writabilityCallbackPending.compareAndSet(false, true)) {
            WebSocketSession<T, D> session = openedSession;

            runCallback(context, () -> {
                writabilityCallbackPending.set(false);
                boolean writable = context.channel().isWritable();

                if (writable != reportedWritable) {
                    reportedWritable = writable;
                    writabilityHandler.handleWritabilityChanged(session, writable);
                }
            });
        }

        context.fireChannelWritabilityChanged();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        handleFailure(context, cause);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext context, Object event) {
        // The aggregator answered a request expecting a continue with 413, because its body would exceed the limit
        if (event instanceof HttpExpectationFailedEvent) {
            notifyObserver(observer -> observer.handshakeRejected(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE.code()));
        }
        context.fireUserEventTriggered(event);
    }

    /**
     * Reports a connection failure and applies the configured close policy.
     *
     * @param context the channel handler context.
     * @param cause   the failure.
     */
    private void handleFailure(ChannelHandlerContext context, Throwable cause) {
        // A broken connection or bytes Netty could not decode are caused by the client.
        // Protocol violations get their own close frame, and a broken connection cannot carry one.
        boolean clientFailure = cause instanceof IOException || cause instanceof DecoderException;
        handleFailure(context, cause, clientFailure, clientFailure ? null : WebSocketCloseStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Reports a failure of an application callback, a server-side failure whatever the type of the exception.
     *
     * @param context the channel handler context.
     * @param failure the failure of the callback.
     */
    private void handleCallbackFailure(ChannelHandlerContext context, Throwable failure) {
        handleFailure(context, failure, false, WebSocketCloseStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Reports a connection failure and applies the configured close policy. Failures caused by the client are logged
     * without a stack trace at debug level, so a client cannot flood the log, the others at error level.
     *
     * @param context       the channel handler context.
     * @param cause         the failure.
     * @param clientFailure whether the client caused the failure.
     * @param closeStatus   the status of the close frame sent when closeOnException is set and the session is open,
     *                      or null to close the connection without one.
     */
    private void handleFailure(
        ChannelHandlerContext context,
        Throwable cause,
        boolean clientFailure,
        WebSocketCloseStatus closeStatus
    ) {
        notifyErrorHandler(cause);
        notifyObserver(observer -> observer.exception(openedSession, cause));

        if (clientFailure) {
            LOGGER.debug("Client failure in channel with id {}: {}", context.channel().id(), cause.toString());
        } else {
            LOGGER.error("Exception caught in channel with id {}", context.channel().id(), cause);
        }

        // The frame decoder leaves protocol violations to this handler, also a Close frame with an invalid status code.
        // The close frame then passes OutboundCloseInfoHandler, so onClose reports its status instead of 1006
        // and a client that does not read cannot keep the close frame and the connection pending.
        // Other decoding failures of an open session, e.g. of the compression decoder, leave its state broken too.
        // An application callback or encoder throwing such an exception is still a server failure.
        if (clientFailure && (cause instanceof CorruptedWebSocketFrameException || cause instanceof DecoderException && openedSession != null)) {
            // A close frame sent before, e.g. 1007 by the UTF-8 validator, closes the connection once it is written
            if (!ClosingHandshake.isClosing(context.channel())) {
                WebSocketCloseStatus status = protocolViolationStatus(cause);
                String reason = cause instanceof CorruptedWebSocketFrameException && cause.getMessage() != null
                    ? cause.getMessage()
                    : status.reasonText();
                context.writeAndFlush(new CloseWebSocketFrame(status, reason))
                    .addListener(ChannelFutureListener.CLOSE);
            }
            return;
        }

        if (!webSocketServer.getConfiguration().isCloseOnException()) {
            return;
        }

        // An open session is closed with a close frame, so the client does not see an abnormal closure
        if (closeStatus != null && openedSession != null && !closeHandled) {
            CloseWebSocketFrame frame = new CloseWebSocketFrame(closeStatus);
            ClosingHandshake.start(context.channel(), frame, webSocketServer.getConfiguration().getCloseTimeout());
        } else {
            context.close();
        }
    }

    /**
     * Returns the close status for a protocol violation of the client.
     * A compressed message that cannot be inflated is reported with the inflater's DataFormatException as the cause,
     * and a message inflating past maxMessageSize without one.
     *
     * @param cause the decoding failure.
     * @return the close status.
     */
    private static WebSocketCloseStatus protocolViolationStatus(Throwable cause) {
        if (cause instanceof DecompressionException) {
            return cause.getCause() instanceof DataFormatException
                ? WebSocketCloseStatus.INVALID_PAYLOAD_DATA
                : WebSocketCloseStatus.MESSAGE_TOO_BIG;
        }
        return cause instanceof CorruptedWebSocketFrameException violation
            ? violation.closeStatus()
            : WebSocketCloseStatus.PROTOCOL_ERROR;
    }

    /**
     * Constructs the WebSocket location URL based on the request.
     *
     * @param request the HTTP request.
     * @return the WebSocket location URL.
     */
    private String getWebSocketLocation(FullHttpRequest request) {
        String location = request.headers().get(HttpHeaderNames.HOST) + webSocketServer.getPath();

        String prefix = webSocketServer.getConfiguration().getTlsConfiguration() != null
            ? "wss"
            : "ws";

        return prefix + "://" + location;
    }

    /**
     * Determines if the HTTP request should be upgraded to WebSocket.
     *
     * @param request the HTTP request.
     * @return true if the request should be upgraded, false otherwise.
     */
    private boolean shouldUpgrade(FullHttpRequest request) {
        if (request.method() != HttpMethod.GET) return false;
        if (request.decoderResult().isFailure()) return false;

        // The opening handshake needs HTTP/1.1 or later and a Host header (RFC 6455, section 4.1),
        // exactly one and with the syntax of a host and an optional port (RFC 9112, section 3.2)
        if (request.protocolVersion().compareTo(HttpVersion.HTTP_1_1) < 0) return false;
        List<String> hosts = request.headers().getAll(HttpHeaderNames.HOST);
        if (hosts.size() != 1 || !HOST.matcher(hosts.getFirst()).matches()) return false;

        // Both headers are comma separated token lists compared case-insensitively (RFC 6455, section 4.2.1)
        HttpHeaders headers = request.headers();
        if (!headers.containsValue(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true)) return false;
        if (!headers.containsValue(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE, true)) return false;

        return webSocketServer.getPath().equals(getRequestPath(request));
    }

    /**
     * Checks that the client requests one of the configured subprotocols.
     * The header may list several, separated by commas (RFC 6455, section 4.1).
     *
     * @param request   the HTTP request.
     * @param supported the configured subprotocols.
     * @return true if one of the requested subprotocols is supported.
     */
    private static boolean requestsSupportedSubprotocol(FullHttpRequest request, List<String> supported) {
        String header = request.headers().get(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL);

        if (header == null) {
            return false;
        }

        for (String requested : header.split(",")) {
            if (supported.contains(requested.trim())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks that the Sec-WebSocket-Key header is a Base64-encoded 16-byte value (RFC 6455, section 4.1).
     *
     * @param request the HTTP request.
     * @return true if the key is valid, false otherwise.
     */
    private static boolean isValidKey(FullHttpRequest request) {
        String key = request.headers().get(HttpHeaderNames.SEC_WEBSOCKET_KEY);

        try {
            return key != null && Base64.getDecoder().decode(key).length == 16;
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    /**
     * Returns the raw path of the request target without its query string.
     * The origin form is accepted, e.g. {@code /chat?token=1}, and the absolute form with an http or https scheme,
     * which RFC 6455 (section 4.1) allows and a client behind a proxy sends.
     * An absolute target is rewritten to its origin form, so the upgrade request sees the same path and query.
     * A fragment is not allowed in either form (RFC 6455, section 3), and a target like {@code //evil.example/chat}
     * parses with an authority, so it does not match {@code /chat} either.
     *
     * @param request the HTTP request.
     * @return the request path, or null if the request target is malformed or in neither form.
     */
    private static String getRequestPath(FullHttpRequest request) {
        URI uri;

        try {
            uri = URI.create(request.uri());
        } catch (IllegalArgumentException ignored) {
            return null;
        }

        if (uri.getRawFragment() != null) {
            return null;
        }

        if (request.uri().startsWith("/")) {
            return uri.getRawAuthority() == null ? uri.getRawPath() : null;
        }

        String scheme = uri.getScheme();
        boolean http = "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);

        if (!http || uri.getRawAuthority() == null || uri.getRawPath() == null) {
            return null;
        }

        // An absolute target without a path means the root (RFC 9112, section 3.2.2)
        String path = uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
        request.setUri(uri.getRawQuery() == null ? path : path + "?" + uri.getRawQuery());
        return path;
    }

    /**
     * Checks if the origin of the request allows WebSocket upgrade.
     *
     * @param request the HTTP request.
     * @return true if the origin is allowed, false otherwise.
     */
    private boolean isUpgradeFromAllowedOrigin(FullHttpRequest request) {
        WebSocketServerConfiguration<T> configuration = webSocketServer.getConfiguration();
        List<String> allowedOrigins = configuration.getAllowedOrigins();
        Pattern allowedOriginPattern = configuration.getAllowedOriginPattern();

        if (allowedOrigins == null && allowedOriginPattern == null) {
            return true;
        }

        String origin = request.headers().get(HttpHeaderNames.ORIGIN);

        if (origin == null) {
            return configuration.isAllowMissingOrigin();
        }

        return allowedOrigins != null && allowedOrigins.contains(origin)
            || allowedOriginPattern != null && allowedOriginPattern.matcher(origin).matches();
    }

    /**
     * Handles close frames received over WebSocket.
     *
     * @param context    the channel handler context.
     * @param closeFrame the close frame received.
     * @param session    the WebSocket session associated with the frame.
     */
    private void handleCloseFrame(ChannelHandlerContext context, CloseWebSocketFrame closeFrame, WebSocketSession<T, D> session) {
        // Netty accepts every code from 3000, but RFC 6455 defines them only up to 4999 (section 7.4.2)
        if (closeFrame.statusCode() > WebSocketSession.MAX_CLOSE_STATUS_CODE) {
            handleFailure(context, new CorruptedWebSocketFrameException(
                WebSocketCloseStatus.PROTOCOL_ERROR,
                "Invalid close status code " + closeFrame.statusCode()
            ));
            return;
        }

        if (!ClosingHandshake.markClientStarted(context.channel())) {
            // The server already sent Close, or this client's Close was handled before.
            // The session is reported as closed with the status of the first Close.
            context.close();
            return;
        }

        // Read the status before echoing the frame, because writing it consumes its content.
        // A close frame without a status code is reported as 1005 (RFC 6455, section 7.1.5).
        int code = closeFrame.statusCode() == -1 ? WebSocketCloseStatus.EMPTY.code() : closeFrame.statusCode();
        String reason = closeFrame.reasonText();

        // The echo is written behind everything already buffered, so a client that stopped reading never receives it
        handshaker.close(context.channel(), closeFrame.retain());
        ClosingHandshake.closeAfter(context.channel(), webSocketServer.getConfiguration().getCloseTimeout());
        handleSessionClosed(context, session, new CloseInfo(code, reason));
    }

    /**
     * Removes the session from the server and its topics, then notifies the close handler.
     * Runs at most once per connection.
     *
     * @param context   the channel handler context.
     * @param session   the WebSocket session that was closed.
     * @param closeInfo the close status reported to the close handler.
     */
    private void handleSessionClosed(ChannelHandlerContext context, WebSocketSession<T, D> session, CloseInfo closeInfo) {
        if (closeHandled) {
            return;
        }

        closeHandled = true;
        webSocketServer.removeSession(context.channel().id());
        notifyObserver(observer -> observer.sessionClosed(session, closeInfo.code(), closeInfo.reason()));

        CloseHandler<T, D> closeHandler = webSocketServer.getCloseHandler();

        if (closeHandler != null) {
            closeCallback = () -> closeHandler.handleClose(session, closeInfo.reason(), closeInfo.code());
            runCallback(context, closeCallback);
        }

        session.markClosed();
    }

    /**
     * Handles message frames received over WebSocket.
     *
     * @param decoder the decoder for WebSocket messages.
     * @param frame   the WebSocket frame received.
     * @param session the WebSocket session associated with the frame.
     */
    private void handleMessageFrame(MessageDecoder<T> decoder, WebSocketFrame frame, WebSocketSession<T, D> session) {
        ByteBuf content = frame.content();
        byte[] bytes = new byte[content.readableBytes()];
        content.getBytes(content.readerIndex(), bytes);

        T message;

        // A message the application cannot decode is data inconsistent with its type (RFC 6455, section 7.4.1)
        try {
            message = decoder.decode(bytes);
        } catch (RuntimeException exception) {
            MessageDecodeException failure = exception instanceof MessageDecodeException decodeException
                ? decodeException
                : new MessageDecodeException("Failed to decode message", bytes, exception);

            handleFailure(session.getChannelContext(), failure, true, WebSocketCloseStatus.INVALID_PAYLOAD_DATA);
            return;
        }
        session.updateLastMessageTime();
        resetIdleTimeout(session.getChannelContext().channel());

        // Checked here, so a server without an observer does not allocate the notification
        if (observer != null) {
            notifyObserver(observer -> observer.messageReceived(session));
        }

        MessageHandler<T, D> messageHandler = webSocketServer.getMessageHandler();

        if (messageHandler != null) {
            runCallback(session.getChannelContext(), () -> messageHandler.handleMessage(session, message));
        }
    }

    /**
     * Handles WebSocket frames.
     *
     * @param context the channel handler context.
     * @param frame   the WebSocket frame.
     */
    private void handleWebSocketFrame(ChannelHandlerContext context, WebSocketFrame frame) {
        WebSocketSession<T, D> session = openedSession;

        if (session == null || closeHandled) {
            LOGGER.warn("Received {} while session is null!", frame.getClass());
            return;
        }

        WebSocketServerConfiguration<T> configuration = webSocketServer.getConfiguration();

        switch (frame) {
            case CloseWebSocketFrame closeFrame -> handleCloseFrame(context, closeFrame, session);

            case PingWebSocketFrame pingFrame -> answerPing(context, pingFrame);

            case PongWebSocketFrame _ ->
                // A Pong needs no response (RFC 6455, section 5.5.3), but it answers a heartbeat ping
                cancelHeartbeatTimeout();

            case TextWebSocketFrame _, BinaryWebSocketFrame _ when ClosingHandshake.isClosing(context.channel()) -> {
                // The session is closing, so messages the client sent before it got the close frame are discarded
            }

            case TextWebSocketFrame textFrame when configuration.isAllowTextFrames() ->
                handleMessageFrame(configuration.getMessageDecoder(), textFrame, session);

            case BinaryWebSocketFrame binaryFrame when configuration.isAllowBinaryFrames() ->
                handleMessageFrame(configuration.getMessageDecoder(), binaryFrame, session);

            case TextWebSocketFrame _, BinaryWebSocketFrame _ -> {
                // The endpoint does not accept this type of data (RFC 6455, section 7.4.1)
                WebSocketCloseStatus status = WebSocketCloseStatus.INVALID_MESSAGE_TYPE;
                session.close(status.code(), status.reasonText());
            }

            // A continuation is assembled before this handler, so one arriving here is out of place
            default -> handleFailure(context, new CorruptedWebSocketFrameException(
                WebSocketCloseStatus.PROTOCOL_ERROR,
                "Unexpected " + frame.getClass().getSimpleName()
            ));
        }
    }

    /**
     * Handles HTTP requests.
     *
     * @param context the channel handler context.
     * @param request the HTTP request.
     */
    private void handleHttpRequest(ChannelHandlerContext context, FullHttpRequest request) {
        // stop() closes only the sessions opened before it, so a stopping server opens no new ones
        if (webSocketServer.isStopping()) {
            sendResponse(context, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }

        if (!shouldUpgrade(request)) {
            sendResponse(context, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        if (!isUpgradeFromAllowedOrigin(request)) {
            sendResponse(context, HttpResponseStatus.FORBIDDEN);
            return;
        }

        WebSocketServerConfiguration<T> configuration = webSocketServer.getConfiguration();

        // Netty selects the first subprotocol requested by the client that is also supported (RFC 6455, section 4.2.2)
        List<String> supportedSubprotocols = configuration.getSubprotocols();
        String subprotocols = supportedSubprotocols.isEmpty() ? null : String.join(",", supportedSubprotocols);

        // Without compression, frames with reserved bits set fail the connection with 1002 (RFC 6455, section 5.2).
        // With it, the decoder lets them through for the decompression, and ReservedBitsValidator rejects the rest.
        boolean allowExtensions = configuration.isCompressionEnabled();

        WebSocketDecoderConfig decoderConfig = WebSocketDecoderConfig.newBuilder()
            .allowExtensions(allowExtensions)
            .maxFramePayloadLength(configuration.getMaxFrameSize())
            .closeOnProtocolViolation(false)
            .build();

        WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
            getWebSocketLocation(request),
            subprotocols,
            decoderConfig
        );

        handshaker = wsFactory.newHandshaker(request);

        // Only RFC 6455 is supported.
        // Netty would also accept the drafts 7 and 8, and Hixie handshakes without a version.
        if (handshaker == null || handshaker.version() != WebSocketVersion.V13) {
            notifyObserver(observer -> observer.handshakeRejected(HttpResponseStatus.UPGRADE_REQUIRED.code()));
            WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(context.channel())
                .addListener(ChannelFutureListener.CLOSE);
            return;
        }

        if (!isValidKey(request)) {
            sendResponse(context, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        // Netty reads only the first line of the header when it selects the subprotocol
        List<String> requestedSubprotocols = request.headers().getAll(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL);
        if (requestedSubprotocols.size() > 1) {
            request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, String.join(", ", requestedSubprotocols));
        }

        if (!supportedSubprotocols.isEmpty() && !requestsSupportedSubprotocol(request, supportedSubprotocols)) {
            sendResponse(context, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        UpgradeHandler<D> upgradeHandler = webSocketServer.getUpgradeHandler();
        D sessionContext = null;

        if (upgradeHandler != null) {
            UpgradeRequest upgradeRequest = createUpgradeRequest(context, request);
            UpgradeResponse upgradeResponse = new UpgradeResponse();
            UpgradeResult<D> result;
            HttpResponse rejection = null;

            // A failing handler, or a rejection with headers Netty refuses, is a server error the client still gets a response to
            try {
                result = upgradeHandler.handleUpgrade(upgradeRequest, upgradeResponse);

                // A missing result rejects the upgrade too, so the client still gets a response
                if (result == null || !result.isAccepted()) {
                    rejection = createHttpResponse(upgradeResponse);
                }
            } catch (RuntimeException exception) {
                // Reported once the response is written, as closeOnException must not drop the pending response
                sendResponse(context, HttpResponseStatus.INTERNAL_SERVER_ERROR)
                    .addListener(_ -> handleCallbackFailure(context, exception));
                return;
            }

            if (rejection != null) {
                int status = upgradeResponse.getStatus();
                notifyObserver(observer -> observer.handshakeRejected(status));
                context.writeAndFlush(rejection).addListener(ChannelFutureListener.CLOSE);
                return;
            }

            sessionContext = result.getContext();
        }

        WebSocketSession<T, D> session = new WebSocketSession<>(
            context,
            messageSender,
            webSocketServer::encode,
            sessionContext,
            configuration.getCloseTimeout()
        );

        ChannelFuture handshakeFuture;

        // Netty validates the request once more before writing the response, and throws on what it rejects
        try {
            handshakeFuture = handshaker.handshake(context.channel(), request);
        } catch (RuntimeException exception) {
            sendResponse(context, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        handshakeFuture.addListener(it -> {
            // The upgrade handler may run past the deadline, and cancelling the overdue timeout would keep the connection
            if (it.isSuccess() && handshakeTimeout != null && handshakeTimeout.getDelay(TimeUnit.NANOSECONDS) <= 0) {
                context.close();
            } else if (it.isSuccess()) {
                cancelHandshakeTimeout();
                // Netty inserts the encoder before its decoder. Intercept closes between them so
                // frames written by the decoder and by every later handler retain their status.
                context.pipeline().addAfter(
                    context.pipeline().context(WebSocketFrameEncoder.class).name(),
                    "outboundCloseInfo",
                    new OutboundCloseInfoHandler(configuration.getCloseTimeout())
                );
                // Replies written while one read batch is handled leave in one write.
                // Added after the handshake, so the HTTP response is flushed as before.
                context.pipeline().addBefore(
                    context.pipeline().context(WebSocketFrameEncoder.class).name(),
                    "flushConsolidation",
                    new FlushConsolidationHandler() {
                        @Override
                        public void flush(ChannelHandlerContext flushContext) throws Exception {
                            // A reply that filled the buffer is written right away instead of at the end of the read,
                            // so the next reply of the batch finds the buffer drained and REJECT_NEW does not fail it
                            if (flushContext.channel().isWritable()) {
                                super.flush(flushContext);
                            } else {
                                flushContext.flush();
                            }
                        }
                    }
                );
                session.setSubprotocol(handshaker.selectedSubprotocol());

                // The handshake completed after stop() took its snapshot of the sessions to close, or the server stopped
                if (!webSocketServer.addSession(session)) {
                    context.writeAndFlush(new CloseWebSocketFrame(WebSocketCloseStatus.ENDPOINT_UNAVAILABLE))
                        .addListener(ChannelFutureListener.CLOSE);
                    return;
                }
                openedSession = session;
                // The channel may already be unwritable, with no later writability change to start the timeout,
                // and the first writability change the callbacks report is relative to this state
                reportedWritable = context.channel().isWritable();
                updateUnwritableTimeout(context.channel());
                resetIdleTimeout(context.channel());
                addHeartbeat(context.pipeline());
                notifyObserver(observer -> observer.sessionOpened(session));
                OpenHandler<T, D> openHandler = webSocketServer.getOpenHandler();
                if (openHandler != null) {
                    runCallback(context, () -> openHandler.handleOpen(session));
                }

                if (framesBeforeOpen != null) {
                    List<WebSocketFrame> frames = framesBeforeOpen;
                    framesBeforeOpen = null;

                    for (WebSocketFrame frame : frames) {
                        handleWebSocketFrame(context, frame);
                        frame.release();
                    }
                    // Flushed like at the end of a read, so a reply such as a Pong does not wait for the next read
                    context.flush();
                }
            } else {
                // The response could not be written, so the client cannot be answered anymore
                context.close();
            }
        });
    }

    private UpgradeRequest createUpgradeRequest(ChannelHandlerContext context, FullHttpRequest request) {
        QueryStringDecoder query = new QueryStringDecoder(request.uri());
        Map<String, String> cookies = new LinkedHashMap<>();

        for (String cookieHeader : request.headers().getAll(HttpHeaderNames.COOKIE)) {
            for (Cookie cookie : ServerCookieDecoder.LAX.decodeAll(cookieHeader)) {
                cookies.put(cookie.name(), cookie.value());
            }
        }

        Map<String, List<String>> headers = new LinkedHashMap<>();
        request.headers().forEach(entry -> headers
            .computeIfAbsent(entry.getKey(), _ -> new ArrayList<>())
            .add(entry.getValue()));

        SocketAddress address = context.channel().remoteAddress();
        InetSocketAddress remoteAddress = address instanceof InetSocketAddress inetAddress ? inetAddress : null;

        SslHandler sslHandler = context.pipeline().get(SslHandler.class);
        SSLSession sslSession = sslHandler == null ? null : sslHandler.engine().getSession();

        return new UpgradeRequest(query.path(), query.parameters(), cookies, headers, remoteAddress, sslSession);
    }

    private HttpResponse createHttpResponse(UpgradeResponse upgradeResponse) {
        HttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.valueOf(upgradeResponse.getStatus())
        );
        upgradeResponse.getHeaders().forEach((name, values) ->
            values.forEach(value -> response.headers().add(name, value))
        );
        return response;
    }

    /**
     * Pings the client whenever nothing was received for the heartbeat interval.
     *
     * @param pipeline the pipeline of the opened WebSocket connection.
     */
    private void addHeartbeat(ChannelPipeline pipeline) {
        WebSocketServerConfiguration<T> configuration = webSocketServer.getConfiguration();
        Duration interval = configuration.getHeartbeatInterval();

        if (interval == null) {
            return;
        }

        Duration timeout = configuration.getHeartbeatTimeout();

        // First in the pipeline, so every read delays the ping, also fragments of a message
        pipeline.addFirst(new IdleStateHandler(interval.toNanos(), 0, 0, TimeUnit.NANOSECONDS) {
            @Override
            protected void channelIdle(ChannelHandlerContext context, IdleStateEvent event) {
                sendHeartbeatPing(context.channel(), timeout);
            }
        });
    }

    /**
     * Pings the client and closes the connection unless a pong arrives within the timeout.
     * Other messages do not answer the ping.
     *
     * @param channel the channel of the WebSocket connection.
     * @param timeout the time to wait for the pong.
     */
    private void sendHeartbeatPing(Channel channel, Duration timeout) {
        // Behind a full write buffer the Pong could not come back in time, and the ping would only add to the buffer.
        // While the server stopped reading for its pending callbacks, the Pong would not be read either,
        // and after a close frame was sent the closing handshake has its own timeout.
        if (!channel.isWritable() || !channel.config().isAutoRead() || ClosingHandshake.isClosing(channel)) {
            return;
        }

        // Written from the tail of the pipeline, so the frame passes the WebSocket encoder
        channel.writeAndFlush(new PingWebSocketFrame());

        // Keep the deadline of an earlier ping that was not answered yet
        if (heartbeatTimeout == null) {
            scheduleHeartbeatTimeout(channel, timeout);
        }
    }

    /**
     * Closes the connection if the Pong does not arrive within the timeout. While the server stopped reading for its
     * pending callbacks, the Pong cannot arrive, so the deadline starts again instead.
     *
     * @param channel the channel of the WebSocket connection.
     * @param timeout the time to wait for the Pong.
     */
    private void scheduleHeartbeatTimeout(Channel channel, Duration timeout) {
        heartbeatTimeout = channel.eventLoop().schedule(() -> {
            if (channel.config().isAutoRead()) {
                closeOnHeartbeatTimeout(channel);
            } else {
                scheduleHeartbeatTimeout(channel, timeout);
            }
        }, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Answers a Ping with a Pong carrying the same payload
     * (RFC 6455, section 5.5.2). The Pong is flushed with the rest of the read.
     * While the channel is unwritable, only the most recent Ping is answered
     * once it is writable again (RFC 6455, section 5.5.3), so a Ping flood
     * cannot grow the write buffer.
     *
     * @param context   the channel handler context.
     * @param pingFrame the received Ping.
     */
    private void answerPing(ChannelHandlerContext context, PingWebSocketFrame pingFrame) {
        // A failed connection sends nothing more, and its validator's close frame may still wait for the flush
        // at the end of the read that also carried this Ping. A graceful close keeps answering until the client's
        // close frame arrives (RFC 6455, section 5.5.2).
        if (!ClosingHandshake.answersPings(context.channel())) {
            return;
        }

        if (context.channel().isWritable()) {
            context.write(new PongWebSocketFrame(pingFrame.content().retain()));
            return;
        }

        releaseDeferredPong();
        deferredPongPayload = pingFrame.content().retain();
    }

    /**
     * Releases the payload of the deferred Pong, if any.
     */
    private void releaseDeferredPong() {
        if (deferredPongPayload != null) {
            deferredPongPayload.release();
            deferredPongPayload = null;
        }
    }

    /**
     * Cancels the pending close after a heartbeat ping, if any.
     */
    private void cancelHeartbeatTimeout() {
        if (heartbeatTimeout != null) {
            heartbeatTimeout.cancel(false);
            heartbeatTimeout = null;
        }
    }

    /**
     * Closes a connection whose heartbeat ping was not answered in time.
     *
     * @param channel the channel of the WebSocket connection.
     */
    private static void closeOnHeartbeatTimeout(Channel channel) {
        // A closed connection keeps the status it was closed with, and a closing one waits for its closing handshake
        if (!channel.isActive() || ClosingHandshake.isClosing(channel)) {
            return;
        }

        CloseInfo closeInfo = new CloseInfo(WebSocketCloseStatus.ENDPOINT_UNAVAILABLE.code(), "Heartbeat timeout");

        // The client may not read anymore, so close without waiting for the close frame to be written
        channel.attr(CloseInfo.KEY).setIfAbsent(closeInfo);
        channel.writeAndFlush(new CloseWebSocketFrame(closeInfo.code(), closeInfo.reason()));
        forceClose(channel);
    }

    /**
     * Restarts the timeout that tracks application-level inactivity.
     * Only successfully decoded text and binary messages call this method after the session is opened.
     * It only records the time, and the pending task checks it when it fires, like Netty's IdleStateHandler.
     *
     * @param channel the channel of the session.
     */
    private void resetIdleTimeout(Channel channel) {
        Duration timeout = webSocketServer.getConfiguration().getIdleTimeout();

        if (timeout == null) {
            return;
        }

        lastActivityNanos = channel.eventLoop().ticker().nanoTime();

        if (idleTimeout == null) {
            scheduleIdleTimeout(channel, timeout.toNanos());
        }
    }

    private void scheduleIdleTimeout(Channel channel, long delayNanos) {
        idleTimeout = channel.eventLoop().schedule(() -> closeOnIdleTimeout(channel), delayNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Cancels the pending handshake timeout, if any.
     */
    private void cancelHandshakeTimeout() {
        if (handshakeTimeout != null) {
            handshakeTimeout.cancel(false);
            handshakeTimeout = null;
        }
    }

    /**
     * Cancels the pending application idle timeout, if any.
     */
    private void cancelIdleTimeout() {
        if (idleTimeout != null) {
            idleTimeout.cancel(false);
            idleTimeout = null;
        }
    }

    /**
     * Closes a session that received no application message before its idle timeout elapsed.
     *
     * @param channel the channel of the session.
     */
    private void closeOnIdleTimeout(Channel channel) {
        idleTimeout = null;

        // The task may run after the socket closed, before channelInactive() cancelled it
        if (!channel.isActive()) {
            return;
        }

        // The client's messages wait unread while the server stopped reading for its pending callbacks
        if (!channel.config().isAutoRead()) {
            resetIdleTimeout(channel);
            return;
        }

        long timeout = webSocketServer.getConfiguration().getIdleTimeout().toNanos();
        long remaining = timeout - (channel.eventLoop().ticker().nanoTime() - lastActivityNanos);

        if (remaining > 0) {
            scheduleIdleTimeout(channel, remaining);
            return;
        }

        Duration closeTimeout = webSocketServer.getConfiguration().getCloseTimeout();
        ClosingHandshake.start(
            channel,
            new CloseWebSocketFrame(WebSocketCloseStatus.ENDPOINT_UNAVAILABLE.code(), "Idle timeout"),
            closeTimeout
        );
    }

    /**
     * Closes the connection once it stays unwritable for the configured time and cancels that close
     * as soon as it is writable again.
     *
     * @param channel the channel of the WebSocket connection.
     */
    private void updateUnwritableTimeout(Channel channel) {
        Duration timeout = webSocketServer.getConfiguration().getUnwritableTimeout();

        if (timeout == null) {
            return;
        }

        if (channel.isWritable()) {
            cancelUnwritableTimeout();
        } else if (unwritableTimeout == null) {
            scheduleUnwritableTimeout(channel, timeout);
        }
    }

    /**
     * Closes the connection if its write buffer does not drain within the timeout.
     * A buffer that shrank meanwhile, e.g. while a slow client reads a large message, starts a new timeout instead.
     *
     * @param channel the channel of the WebSocket connection.
     * @param timeout the time the buffer may not drain.
     */
    private void scheduleUnwritableTimeout(Channel channel, Duration timeout) {
        unwritablePendingBytes = pendingWriteBytes(channel);
        unwritableTimeout = channel.eventLoop().schedule(() -> {
            if (pendingWriteBytes(channel) < unwritablePendingBytes) {
                scheduleUnwritableTimeout(channel, timeout);
            } else {
                closeUnwritable(channel);
            }
        }, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Returns the bytes still to be written, counting the part of the current message the transport already took.
     * A large message leaves the pending total unchanged until it is written completely, so without its progress a
     * slow client reading it over several timeouts would be closed although the buffer drains.
     */
    private static long pendingWriteBytes(Channel channel) {
        ChannelOutboundBuffer buffer = channel.unsafe().outboundBuffer();
        return buffer == null ? 0 : buffer.totalPendingWriteBytes() - buffer.currentProgress();
    }

    /**
     * Closes the transport right away. Closing through the pipeline would let the TLS handler wait up to three
     * seconds for its close_notify, which cannot leave a full write buffer of a client that does not read.
     *
     * @param channel the channel of the WebSocket connection.
     */
    private static void forceClose(Channel channel) {
        channel.unsafe().close(channel.voidPromise());
    }

    /**
     * Cancels the pending close of an unwritable connection, if any.
     */
    private void cancelUnwritableTimeout() {
        if (unwritableTimeout != null) {
            unwritableTimeout.cancel(false);
            unwritableTimeout = null;
        }
    }

    /**
     * Closes a connection that stayed unwritable for too long. Its write buffer is full,
     * so no close frame is sent and the closure is reported as abnormal.
     *
     * @param channel the channel of the WebSocket connection.
     */
    private static void closeUnwritable(Channel channel) {
        // The task may run after the socket closed, before channelInactive() cancelled it
        if (!channel.isActive()) {
            return;
        }

        CloseInfo closeInfo = new CloseInfo(WebSocketCloseStatus.ABNORMAL_CLOSURE.code(), "Unwritable timeout");

        channel.attr(CloseInfo.KEY).setIfAbsent(closeInfo);
        forceClose(channel);
    }

    /**
     * Runs an application callback on the callback executor after the earlier callbacks of this connection,
     * or right away on the event loop if no executor is configured.
     * A failure of the callback is reported like any other connection failure, whatever thread the callback ran on.
     *
     * @param context  the channel handler context.
     * @param callback the application callback.
     */
    private void runCallback(ChannelHandlerContext context, Runnable callback) {
        if (callbackExecutor == null) {
            try {
                callback.run();
            } catch (Throwable failure) {
                handleCallbackFailure(context, failure);
            }
            return;
        }

        int maxPending = webSocketServer.getConfiguration().getMaxPendingCallbacks();

        if (pendingCallbacks.incrementAndGet() >= maxPending) {
            ReadGate.setReading(context, false);
        }

        callbackExecutor.execute(() -> {
            try {
                callback.run();
            } catch (Throwable failure) {
                // The next callbacks still run
                reportCallbackFailure(context, failure);
            } finally {
                // A terminated event loop reads nothing more, and the rejection must not escape the callback executor
                if (pendingCallbacks.decrementAndGet() == maxPending / 2) {
                    onEventLoop(context, () -> resumeReading(context, maxPending), () -> {});
                }
            }
        }, exception -> {
            // A rejected callback never runs, so it must not keep the connection from reading
            pendingCallbacks.decrementAndGet();
            onEventLoop(context, () -> {
                resumeReading(context, maxPending);
                handleRejectedCallback(context, callback, exception);
            }, () -> handleRejectedCallbackAfterTermination(context, callback, exception));
        });
    }

    /**
     * Runs the task on the event loop, right away if called on it.
     * Once the event loop is terminated, runs the fallback on the calling thread instead.
     *
     * @param context  the channel handler context.
     * @param task     the task to run on the event loop.
     * @param fallback the task to run if the event loop is terminated.
     */
    private static void onEventLoop(ChannelHandlerContext context, Runnable task, Runnable fallback) {
        if (context.executor().inEventLoop()) {
            task.run();
            return;
        }

        try {
            context.executor().execute(task);
        } catch (RejectedExecutionException exception) {
            try {
                fallback.run();
            } catch (Throwable failure) {
                LOGGER.error("Callback failed in channel with id {}", context.channel().id(), failure);
            }
        }
    }

    /**
     * Reports a callback failure on the event loop, where the handler state may be accessed.
     * Once the event loop is terminated, only the error handler and the observer are notified.
     *
     * @param context the channel handler context.
     * @param failure the failure of the callback.
     */
    private void reportCallbackFailure(ChannelHandlerContext context, Throwable failure) {
        try {
            context.executor().execute(() -> handleCallbackFailure(context, failure));
        } catch (RejectedExecutionException exception) {
            LOGGER.error("Callback failed in channel with id {}", context.channel().id(), failure);
            notifyErrorHandler(failure);
            notifyObserver(observer -> observer.exception(openedSession, failure));
        }
    }

    /**
     * Handles a callback the callback executor did not accept.
     * The close callback still runs on the event loop, so the application can clean up, the other callbacks
     * of a closed or closing session are dropped, and so are the ones queued behind the first rejection.
     * The first rejection is reported, and it closes an open session with 1013 (try again later),
     * whatever the closeOnException setting.
     *
     * @param context   the channel handler context.
     * @param callback  the rejected callback.
     * @param exception the rejection.
     */
    private void handleRejectedCallback(ChannelHandlerContext context, Runnable callback, RejectedExecutionException exception) {
        if (callback == closeCallback) {
            try {
                callback.run();
            } catch (Throwable failure) {
                handleCallbackFailure(context, failure);
            }
            return;
        }

        if (rejectionReported) {
            return;
        }
        rejectionReported = true;

        // Reported without the close policy, which could close the connection before the close frame is sent
        LOGGER.error("Callback executor rejected a callback in channel with id {}", context.channel().id(), exception);
        notifyErrorHandler(exception);
        notifyObserver(observer -> observer.exception(openedSession, exception));

        if (closeHandled || ClosingHandshake.isClosing(context.channel())) {
            return;
        }

        if (openedSession != null) {
            CloseWebSocketFrame frame = new CloseWebSocketFrame(WebSocketCloseStatus.TRY_AGAIN_LATER);
            ClosingHandshake.start(context.channel(), frame, webSocketServer.getConfiguration().getCloseTimeout());
        } else {
            context.close();
        }
    }

    /**
     * Handles a rejected callback once the event loop is terminated, e.g. after the server stopped.
     * Only the close callback runs, on the rejecting thread, and like in {@link #reportCallbackFailure}
     * only the error handler and the observer are notified.
     *
     * @param context   the channel handler context.
     * @param callback  the rejected callback.
     * @param exception the rejection.
     */
    private void handleRejectedCallbackAfterTermination(ChannelHandlerContext context, Runnable callback, RejectedExecutionException exception) {
        if (callback != closeCallback) {
            LOGGER.error("Callback executor rejected a callback in channel with id {}", context.channel().id(), exception);
            notifyErrorHandler(exception);
            notifyObserver(observer -> observer.exception(openedSession, exception));
            return;
        }

        try {
            callback.run();
        } catch (Throwable failure) {
            LOGGER.error("Callback failed in channel with id {}", context.channel().id(), failure);
            notifyErrorHandler(failure);
            notifyObserver(observer -> observer.exception(openedSession, failure));
        }
    }

    /**
     * Resumes reading from the connection once the pending callbacks dropped to half of the maximum.
     * The client's Pong and messages waited unread meanwhile, so the heartbeat and idle deadlines start again,
     * otherwise a deadline firing right after the resume would close a healthy client.
     *
     * @param context    the channel handler context.
     * @param maxPending the maximum number of pending callbacks.
     */
    private void resumeReading(ChannelHandlerContext context, int maxPending) {
        Channel channel = context.channel();

        if (pendingCallbacks.get() > maxPending / 2 || channel.config().isAutoRead()) {
            return;
        }

        ReadGate.setReading(context, true);

        if (heartbeatTimeout != null) {
            cancelHeartbeatTimeout();
            scheduleHeartbeatTimeout(channel, webSocketServer.getConfiguration().getHeartbeatTimeout());
        }
        resetIdleTimeout(channel);
    }

    /**
     * Notifies the observer, if any. Its exceptions are only logged, so a failing observer never affects the connection.
     *
     * @param notification the call of the observer.
     */
    private void notifyObserver(Consumer<WebSocketServerObserver<T, D>> notification) {
        if (observer == null) {
            return;
        }

        try {
            notification.accept(observer);
        } catch (Throwable exception) {
            // Not passed to exceptionCaught(), which could close the connection
            LOGGER.warn("WebSocket server observer failed", exception);
        }
    }

    /**
     * Notifies the application error handler, if any. Its exceptions are only logged to avoid recursive failure
     * handling.
     *
     * @param exception the connection failure.
     */
    private void notifyErrorHandler(Throwable exception) {
        if (errorHandler == null) {
            return;
        }

        try {
            errorHandler.handle(openedSession, exception);
        } catch (Throwable handlerException) {
            LOGGER.warn("WebSocket error handler failed", handlerException);
        }
    }

    /**
     * Sends a response with the given status to the client and closes the connection.
     *
     * @param context the channel handler context.
     * @param status  the response status.
     * @return the future of the write, notified after the connection is closed.
     */
    private ChannelFuture sendResponse(ChannelHandlerContext context, HttpResponseStatus status) {
        notifyObserver(observer -> observer.handshakeRejected(status.code()));
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
        return context.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
}
