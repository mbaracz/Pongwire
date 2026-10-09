package pl.mbaracz.jwebsockets;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.util.concurrent.EventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.mbaracz.jwebsockets.configuration.WebSocketServerConfiguration;
import pl.mbaracz.jwebsockets.configuration.WebSocketServerConfigurer;
import pl.mbaracz.jwebsockets.handler.CloseHandler;
import pl.mbaracz.jwebsockets.handler.ErrorHandler;
import pl.mbaracz.jwebsockets.handler.MessageHandler;
import pl.mbaracz.jwebsockets.handler.OpenHandler;
import pl.mbaracz.jwebsockets.handler.UpgradeHandler;
import pl.mbaracz.jwebsockets.handler.WritabilityHandler;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * WebSocketServer represents a WebSocket server that listens for incoming WebSocket connections.
 * It allows configuring message, lifecycle, error, and upgrade handlers.
 *
 * @param <T> Type of messages to be handled by the server
 * @param <D> Type of additional data associated with the session
 */
public class WebSocketServer<T, D> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServer.class);

    // Upper bound for closing client connections and for event loop termination in stop()
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

    private final String path;
    private OpenHandler<T, D> openHandler;
    private UpgradeHandler<D> upgradeHandler;
    private CloseHandler<T, D> closeHandler;
    private MessageHandler<T, D> messageHandler;
    private ErrorHandler<T, D> errorHandler;
    private WritabilityHandler<T, D> writabilityHandler;
    private WebSocketServerObserver<T, D> observer;

    // Guards listen() and stop() without blocking session registry operations,
    // so stopping the server never waits on a lock held by session callbacks.
    private final Object lifecycleLock = new Object();
    private volatile State state = State.STOPPED;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private volatile Channel serverChannel;

    private volatile TopicBroker<T> topicBroker = new InMemoryTopicBroker<>();
    private volatile WebSocketServerConfiguration<T> configuration = WebSocketServerConfiguration.<T>builder().build();
    private final SessionRegistry<T, D> sessionRegistry = new SessionRegistry<>();
    private final SessionTopicRegistry<T, D> sessionTopicRegistry = new SessionTopicRegistry<>();
    private final TopicMessageHandler<T> topicMessageHandler = this::handleTopicMessage;
    private final Map<String, CompletableFuture<Void>> topicBrokerOperations = new HashMap<>();
    private final Map<String, BrokerSubscription> brokerSubscriptions = new HashMap<>();
    private final Object sessionTopicLock = new Object();

    private enum State {
        STOPPED,
        RUNNING,
        STOPPING
    }

    private enum BrokerSubscriptionStatus {
        UNSUBSCRIBED,
        SUBSCRIBING,
        SUBSCRIBED,
        UNSUBSCRIBING
    }

    /**
     * Default constructor initializing the WebSocket server with the root path.
     */
    public WebSocketServer() {
        this("/");
    }

    /**
     * Constructor initializing the WebSocket server with a specified path.
     *
     * @param path The path for the WebSocket server
     * @throws IllegalArgumentException If the path is null, does not start with {@code /}, or contains a query or fragment
     */
    public WebSocketServer(String path) {
        if (path == null || !path.startsWith("/") || path.contains("?") || path.contains("#")) {
            throw new IllegalArgumentException(
                "WebSocket path must start with '/' and must not contain query or fragment"
            );
        }
        this.path = path;
    }

    /**
     * Configures the WebSocket server using the provided configurer.
     * The configurer gets a builder with the current settings, and the server keeps the configuration built from it.
     *
     * @param configurer Configurer for WebSocket server
     * @return The WebSocket server instance for method chaining
     * @throws IllegalStateException If the server is running or stopping
     */
    public WebSocketServer<T, D> configure(WebSocketServerConfigurer<T> configurer) {
        synchronized (lifecycleLock) {
            ensureConfigurable();

            WebSocketServerConfiguration.Builder<T> builder = configuration.toBuilder();
            configurer.configure(builder);
            configuration = builder.build();
        }
        return this;
    }

    /**
     * Sets the message handler for processing incoming messages.
     *
     * @param handler Message handler to be set
     * @return The WebSocket server instance for method chaining
     * @throws IllegalStateException If the server is running or stopping
     */
    public WebSocketServer<T, D> onMessage(MessageHandler<T, D> handler) {
        synchronized (lifecycleLock) {
            ensureConfigurable();
            this.messageHandler = handler;
        }
        return this;
    }

    /**
     * Sets the application handler for errors reported by WebSocket connections.
     * The handler is invoked on the connection's event loop and should not block.
     * Its exceptions are logged and do not affect the connection or its configured close policy.
     *
     * @param handler Error handler to be set
     * @return The WebSocket server instance for method chaining
     * @throws IllegalStateException If the server is running or stopping
     */
    public WebSocketServer<T, D> onError(ErrorHandler<T, D> handler) {
        synchronized (lifecycleLock) {
            ensureConfigurable();
            this.errorHandler = handler;
        }
        return this;
    }

    /**
     * Sets the close handler for handling WebSocket session closure.
     *
     * @param handler Close handler to be set
     * @return The WebSocket server instance for method chaining
     * @throws IllegalStateException If the server is running or stopping
     */
    public WebSocketServer<T, D> onClose(CloseHandler<T, D> handler) {
        synchronized (lifecycleLock) {
            ensureConfigurable();
            this.closeHandler = handler;
        }
        return this;
    }

    /**
     * Sets the open handler for handling new WebSocket connections.
     *
     * @param handler Open handler to be set
     * @return The WebSocket server instance for method chaining
     * @throws IllegalStateException If the server is running or stopping
     */
    public WebSocketServer<T, D> onOpen(OpenHandler<T, D> handler) {
        synchronized (lifecycleLock) {
            ensureConfigurable();
            this.openHandler = handler;
        }
        return this;
    }

    /**
     * Sets the upgrade handler for handling WebSocket protocol upgrade requests.
     *
     * @param handler Upgrade handler to be set
     * @return The WebSocket server instance for method chaining
     * @throws IllegalStateException If the server is running or stopping
     */
    public WebSocketServer<T, D> onUpgrade(UpgradeHandler<D> handler) {
        synchronized (lifecycleLock) {
            ensureConfigurable();
            this.upgradeHandler = handler;
        }
        return this;
    }

    /**
     * Sets the handler for changes of WebSocket session writability.
     * Without a callback executor the handler runs on the connection's event loop and should not block.
     *
     * @param handler Writability handler to be set
     * @return The WebSocket server instance for method chaining
     * @throws IllegalStateException If the server is running or stopping
     */
    public WebSocketServer<T, D> onWritabilityChanged(WritabilityHandler<T, D> handler) {
        synchronized (lifecycleLock) {
            ensureConfigurable();
            this.writabilityHandler = handler;
        }
        return this;
    }

    /**
     * Sets the topic broker transporting published messages, an in-memory broker by default.
     *
     * @param topicBroker Topic broker to be used
     * @return The WebSocket server instance for method chaining
     * @throws IllegalStateException If the server is running or stopping
     */
    public WebSocketServer<T, D> topicBroker(TopicBroker<T> topicBroker) {
        Objects.requireNonNull(topicBroker, "Topic broker must not be null!");

        synchronized (lifecycleLock) {
            ensureConfigurable();

            synchronized (sessionTopicLock) {
                if (this.topicBroker != topicBroker && hasActiveBrokerState()) {
                    throw new IllegalStateException(
                        "Topic broker cannot be replaced while subscriptions or operations are still active"
                    );
                }

                this.topicBroker = topicBroker;
            }
        }
        return this;
    }

    private boolean hasActiveBrokerState() {
        return !topicBrokerOperations.isEmpty()
            || brokerSubscriptions.values().stream()
                .anyMatch(subscription -> subscription.status != BrokerSubscriptionStatus.UNSUBSCRIBED);
    }

    /**
     * Sets the observer notified about sessions, messages and exceptions, e.g. to record metrics.
     * The observer is invoked on the connection's event loop and should not block.
     *
     * @param observer Observer to be set
     * @return The WebSocket server instance for method chaining
     * @throws IllegalStateException If the server is running or stopping
     */
    public WebSocketServer<T, D> observer(WebSocketServerObserver<T, D> observer) {
        synchronized (lifecycleLock) {
            ensureConfigurable();
            this.observer = observer;
        }
        return this;
    }

    private void ensureConfigurable() {
        if (state != State.STOPPED) {
            throw new IllegalStateException("Server cannot be reconfigured while running or stopping!");
        }
    }

    /**
     * Subscribes a WebSocket session to a given topic.
     *
     * @param session The WebSocket session to subscribe.
     * @param topic   The topic to subscribe the session to.
     * @return A stage completed when any required broker subscription has completed
     * @throws IllegalStateException If the session is not connected to this server
     */
    public CompletionStage<Void> subscribe(WebSocketSession<T, D> session, String topic) {
        PendingBrokerOperation operation = null;
        CompletionStage<Void> result;

        synchronized (sessionTopicLock) {
            if (!sessionRegistry.contains(session)) {
                throw new IllegalStateException("WebSocket session is not connected");
            }

            sessionTopicRegistry.subscribe(session, topic);

            BrokerSubscription subscription = brokerSubscriptions.computeIfAbsent(
                topic,
                _ -> new BrokerSubscription()
            );

            if (subscription.status == BrokerSubscriptionStatus.SUBSCRIBED) {
                result = CompletableFuture.completedFuture(null);
            } else if (subscription.status == BrokerSubscriptionStatus.SUBSCRIBING) {
                result = subscription.transition;
            } else if (subscription.status == BrokerSubscriptionStatus.UNSUBSCRIBING) {
                operation = prepareBrokerSubscribeAfterUnsubscribe(topic, subscription);
                result = operation.result;
            } else {
                TopicBroker<T> broker = topicBroker;
                operation = prepareBrokerOperation(topic, () -> broker.subscribe(topic, topicMessageHandler));
                result = trackBrokerTransition(
                    topic,
                    subscription,
                    operation.result,
                    BrokerSubscriptionStatus.SUBSCRIBING,
                    BrokerSubscriptionStatus.SUBSCRIBED,
                    BrokerSubscriptionStatus.UNSUBSCRIBED,
                    broker
                );
            }
        }

        start(operation);
        return result;
    }

    /**
     * Checks if a WebSocket session is subscribed to a given topic.
     *
     * @param session The WebSocket session to check.
     * @param topic   The topic to check the subscription for.
     * @return true if the session is subscribed to the topic, false otherwise.
     */
    public boolean isSubscribed(WebSocketSession<T, D> session, String topic) {
        return sessionTopicRegistry.isSubscribed(session, topic);
    }

    /**
     * Unsubscribes a WebSocket session from a given topic.
     *
     * @param session The WebSocket session to unsubscribe.
     * @param topic   The topic to unsubscribe the session from.
     * @return A stage completed when any required broker unsubscription has completed
     */
    public CompletionStage<Void> unsubscribe(WebSocketSession<T, D> session, String topic) {
        BrokerTransition transition;

        synchronized (sessionTopicLock) {
            sessionTopicRegistry.unsubscribe(session, topic);

            transition = !sessionTopicRegistry.hasSubscribers(topic)
                ? prepareBrokerUnsubscribe(topic)
                : BrokerTransition.completed();
        }

        return transition.start();
    }

    /**
     * Unsubscribes all WebSocket sessions from all topics.
     *
     * @return A stage completed when all broker unsubscriptions have completed
     */
    public CompletionStage<Void> unsubscribeAllTopics() {
        List<BrokerTransition> transitions = new ArrayList<>();

        synchronized (sessionTopicLock) {
            Set<String> topics = new HashSet<>(sessionTopicRegistry.getTopics());
            topics.addAll(brokerSubscriptions.keySet());

            for (String topic : topics) {
                transitions.add(prepareBrokerUnsubscribe(topic));
            }
            sessionTopicRegistry.clear();
        }

        return startAll(transitions);
    }

    /**
     * Publishes a message to all WebSocket sessions subscribed to a given topic.
     *
     * @param topic   The topic to which the message will be published.
     * @param message The message to be published.
     * @return A stage completed when the broker has accepted the message
     */
    public CompletionStage<Void> publish(String topic, T message) {
        PendingBrokerOperation operation;

        synchronized (sessionTopicLock) {
            TopicBroker<T> broker = topicBroker;
            operation = prepareBrokerOperation(topic, () -> broker.publish(topic, message));
        }

        return operation.start();
    }

    /**
     * Retrieves a snapshot of all the topics to which WebSocket sessions are subscribed.
     * The returned set is unmodifiable and does not change when sessions subscribe or unsubscribe later.
     *
     * @return A set of topics
     */
    public Set<String> getTopics() {
        return sessionTopicRegistry.getTopics();
    }

    /**
     * Delivers a topic message to the subscribers with one task per event loop, so the publishing thread does not
     * write to every subscriber itself. The task is queued even on the current event loop, so each subscriber
     * receives the messages of a topic in the order of the broker.
     *
     * @param topic   the topic of the message.
     * @param message the message.
     */
    private void handleTopicMessage(String topic, T message) {
        Map<EventExecutor, List<WebSocketSession<T, D>>> subscribersByLoop = new HashMap<>();

        for (WebSocketSession<T, D> session : sessionTopicRegistry.getSubscribers(topic)) {
            subscribersByLoop.computeIfAbsent(session.getChannelContext().executor(), _ -> new ArrayList<>()).add(session);
        }

        subscribersByLoop.forEach((loop, sessions) -> {
            try {
                loop.execute(() -> sessions.forEach(session -> session.sendMessage(message)));
            } catch (RejectedExecutionException exception) {
                // The event loop is shutting down and closes its sessions
                LOGGER.debug("Dropped topic message for {} sessions on a terminated event loop", sessions.size());
            }
        });
    }

    private PendingBrokerOperation prepareBrokerSubscribeAfterUnsubscribe(
        String topic,
        BrokerSubscription subscription
    ) {
        CompletionStage<Void> unsubscribing = subscription.transition;
        TopicBroker<T> subscribedBroker = subscription.broker;
        TopicBroker<T> broker = topicBroker;
        AtomicBoolean unsubscribeFailed = new AtomicBoolean();
        PendingBrokerOperation operation = prepareBrokerOperation(topic,
            () -> unsubscribing.handle((_, failure) -> failure)
                .thenCompose(failure -> {
                    if (failure != null) {
                        unsubscribeFailed.set(true);
                        synchronized (sessionTopicLock) {
                            subscription.broker = subscribedBroker;
                        }
                        return CompletableFuture.completedFuture(null);
                    }
                    return broker.subscribe(topic, topicMessageHandler)
                        .thenRun(() -> {
                            synchronized (sessionTopicLock) {
                                subscription.broker = broker;
                            }
                        });
                }));

        subscription.status = BrokerSubscriptionStatus.SUBSCRIBING;
        subscription.transition = operation.result;
        operation.result.whenComplete((_, failure) -> {
            synchronized (sessionTopicLock) {
                if (subscription.transition != operation.result) {
                    return;
                }

                subscription.transition = null;

                subscription.status = failure == null
                    ? BrokerSubscriptionStatus.SUBSCRIBED
                    : BrokerSubscriptionStatus.UNSUBSCRIBED;

                subscription.broker = failure == null
                    ? (unsubscribeFailed.get() ? subscribedBroker : broker)
                    : null;
            }
        });
        return operation;
    }

    private BrokerTransition prepareBrokerUnsubscribe(String topic) {
        BrokerSubscription subscription = brokerSubscriptions.get(topic);

        if (subscription == null || subscription.status == BrokerSubscriptionStatus.UNSUBSCRIBED) {
            brokerSubscriptions.remove(topic);
            return BrokerTransition.completed();
        }

        if (subscription.status == BrokerSubscriptionStatus.UNSUBSCRIBING) {
            return new BrokerTransition(subscription.transition, null);
        }

        TopicBroker<T> broker = subscription.broker;
        Supplier<CompletionStage<Void>> invocation = getCompletionStageSupplier(topic, subscription, broker);
        PendingBrokerOperation operation = prepareBrokerOperation(topic, invocation);

        CompletionStage<Void> result = trackBrokerTransition(
            topic,
            subscription,
            operation.result,
            BrokerSubscriptionStatus.UNSUBSCRIBING,
            BrokerSubscriptionStatus.UNSUBSCRIBED,
            BrokerSubscriptionStatus.SUBSCRIBED,
            broker
        );

        return new BrokerTransition(result, operation);
    }

    private Supplier<CompletionStage<Void>> getCompletionStageSupplier(
        String topic,
        BrokerSubscription subscription,
        TopicBroker<T> broker
    ) {
        CompletionStage<Void> subscribing = subscription.status == BrokerSubscriptionStatus.SUBSCRIBING
            ? subscription.transition
            : null;

        if (subscribing == null) {
            return () -> broker.unsubscribe(topic, topicMessageHandler);
        }

        return () -> subscribing.handle((_, failure) -> failure).thenCompose(failure -> {
            if (failure != null) {
                return CompletableFuture.completedFuture(null);
            }

            TopicBroker<T> activeBroker;

            synchronized (sessionTopicLock) {
                activeBroker = subscription.broker;
            }
            return activeBroker.unsubscribe(topic, topicMessageHandler);
        });
    }

    private CompletionStage<Void> trackBrokerTransition(
        String topic,
        BrokerSubscription subscription,
        CompletableFuture<Void> transition,
        BrokerSubscriptionStatus pendingStatus,
        BrokerSubscriptionStatus successStatus,
        BrokerSubscriptionStatus failureStatus,
        TopicBroker<T> broker
    ) {
        subscription.status = pendingStatus;
        subscription.transition = transition;
        subscription.broker = broker;

        transition.whenComplete((_, failure) -> {
            synchronized (sessionTopicLock) {
                if (subscription.transition != transition) {
                    return;
                }

                subscription.status = failure == null ? successStatus : failureStatus;
                subscription.transition = null;

                if (subscription.status == BrokerSubscriptionStatus.UNSUBSCRIBED) {
                    subscription.broker = null;

                    if (!sessionTopicRegistry.hasSubscribers(topic)) {
                        brokerSubscriptions.remove(topic, subscription);
                    }
                }
            }
        });
        return transition;
    }

    /**
     * Adds a broker operation to the per-topic chain while the caller holds {@link #sessionTopicLock}.
     * The returned operation must be started after releasing that lock, so even a synchronous broker
     * implementation is never invoked while local session state is locked.
     */
    private PendingBrokerOperation prepareBrokerOperation(
        String topic,
        Supplier<CompletionStage<Void>> invocation
    ) {
        Objects.requireNonNull(topic, "Topic must not be null!");

        CompletableFuture<Void> trigger = new CompletableFuture<>();

        CompletableFuture<Void> previous = topicBrokerOperations.get(topic);

        CompletionStage<Void> predecessor = previous == null
            ? CompletableFuture.completedFuture(null)
            : previous.handle((_, _) -> null);

        CompletableFuture<Void> result = predecessor
            .thenCompose(_ -> trigger)
            .thenCompose(_ -> invokeBroker(invocation))
            .toCompletableFuture();

        topicBrokerOperations.put(topic, result);

        result.whenComplete((_, _) -> {
            synchronized (sessionTopicLock) {
                topicBrokerOperations.remove(topic, result);
            }
        });

        return new PendingBrokerOperation(result, trigger);
    }

    private static CompletionStage<Void> invokeBroker(Supplier<CompletionStage<Void>> invocation) {
        try {
            return Objects.requireNonNull(invocation.get(), "Topic broker returned a null CompletionStage");
        } catch (Throwable throwable) {
            return CompletableFuture.failedFuture(throwable);
        }
    }

    private static CompletionStage<Void> start(PendingBrokerOperation operation) {
        return operation == null ? CompletableFuture.completedFuture(null) : operation.start();
    }

    private static CompletionStage<Void> startAll(List<BrokerTransition> transitions) {
        CompletableFuture<?>[] results = transitions.stream()
            .map(BrokerTransition::start)
            .map(CompletionStage::toCompletableFuture)
            .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(results);
    }

    private final class BrokerSubscription {

        private BrokerSubscriptionStatus status = BrokerSubscriptionStatus.UNSUBSCRIBED;
        private CompletableFuture<Void> transition;
        private TopicBroker<T> broker;
    }

    private record BrokerTransition(CompletionStage<Void> result, PendingBrokerOperation operation) {

        static BrokerTransition completed() {
            return new BrokerTransition(CompletableFuture.completedFuture(null), null);
        }

        CompletionStage<Void> start() {
            if (operation != null) {
                operation.start();
            }
            return result;
        }
    }

    private record PendingBrokerOperation(CompletableFuture<Void> result, CompletableFuture<Void> trigger) {
        CompletionStage<Void> start() {
            trigger.complete(null);
            return result;
        }
    }

    /**
     * Starts the WebSocket server and listens for incoming connections on the specified port.
     * Returns once the server is bound to the port.
     *
     * @param port Port number to listen on
     * @return The WebSocket server instance for method chaining
     * @throws IllegalStateException    If the server is already running or still stopping, message encoder/decoder
     *                                  was not provided or the server could not be bound to the port
     * @throws IllegalArgumentException If the port is outside the valid range
     */
    public WebSocketServer<T, D> listen(int port) throws IllegalStateException {
        synchronized (lifecycleLock) {
            if (state == State.RUNNING && serverChannel == null) {
                throw new IllegalStateException("WebSocket server channel closed unexpectedly, call stop() before starting it again!");
            }
            if (state == State.RUNNING) {
                throw new IllegalStateException("WebSocket server is already running at " + serverChannel.localAddress() + "!");
            }
            if (state == State.STOPPING) {
                throw new IllegalStateException("WebSocket server is still stopping, cannot start it yet!");
            }
            if (configuration.getMessageDecoder() == null) {
                throw new IllegalStateException("Message decoder is not provided, cannot start the server!");
            }
            if (configuration.getMessageEncoder() == null) {
                throw new IllegalStateException("Message encoder is not provided, cannot start the server!");
            }

            EventLoopGroup boss = null;
            EventLoopGroup worker = null;
            boolean started = false;

            try {
                boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
                worker = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

                ChannelFuture bindFuture = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new WebSocketServerChannelInitializer<>(this))
                    .bind(port)
                    .awaitUninterruptibly();

                if (!bindFuture.isSuccess()) {
                    throw new IllegalStateException("Failed to start WebSocket server on port " + port, bindFuture.cause());
                }

                bossGroup = boss;
                workerGroup = worker;
                serverChannel = bindFuture.channel();
                state = State.RUNNING;
                started = true;
            } finally {
                // Release the event loops of a failed startup, also when it fails before the bind result,
                // e.g. bind(int) throwing for an invalid port
                if (!started) {
                    shutdownGracefully(boss, worker);
                }
            }

            Channel channel = serverChannel;

            channel.closeFuture().addListener(_ -> {
                synchronized (lifecycleLock) {
                    if (serverChannel == channel) {
                        serverChannel = null;

                        if (state == State.RUNNING) {
                            LOGGER.warn("WebSocket server channel closed unexpectedly, call stop() to release the server's resources");
                        }
                    }
                }
            });

            LOGGER.info("Started WebSocket server at {}", formatStartupAddress((InetSocketAddress) channel.localAddress()));

            return this;
        }
    }

    /**
     * Stops the WebSocket server gracefully.
     * Closes the server channel, sends a going away close frame to every connected session and closes its connection,
     * then shuts down the event loop groups and waits for them to terminate and for the pending topic broker
     * operations, each for a limited time. Also releases a server whose channel closed unexpectedly.
     * With a callback executor, onClose callbacks may run after this method returns, so shut the executor down
     * gracefully afterwards.
     *
     * @throws IllegalStateException If the server is not running or the method is called from one of the server's
     *                               event loop threads, where waiting for the shutdown would block forever
     */
    public void stop() {
        Channel channel;
        EventLoopGroup boss;
        EventLoopGroup worker;

        synchronized (lifecycleLock) {
            if (state != State.RUNNING) {
                throw new IllegalStateException("Server is not running!");
            }
            if (isEventLoopThread(bossGroup, workerGroup)) {
                throw new IllegalStateException("Server cannot be stopped from its own event loop thread!");
            }
            LOGGER.info("Stopping WebSocket server...");

            // STOPPING keeps listen() out until the resources below are released
            state = State.STOPPING;
            channel = serverChannel;
            boss = bossGroup;
            worker = workerGroup;
        }

        // Wait outside the lock, because the server channel close listener
        // runs on the boss event loop and needs the lock to complete.
        // The channel is null if it was closed without calling stop().
        try {
            if (channel != null) {
                channel.close().awaitUninterruptibly();
            }
            closeSessions();
        } finally {
            // Release the event loops and leave STOPPING even if closing the connections failed
            try {
                shutdownGracefully(boss, worker);
                settleBrokerState();
            } finally {
                synchronized (lifecycleLock) {
                    serverChannel = null;
                    bossGroup = null;
                    workerGroup = null;
                    state = State.STOPPED;
                }
            }
        }

        LOGGER.info("Server stopped!");
    }

    /**
     * Waits, within the close timeout, for the broker operations the closed sessions started, then forgets the broker
     * state of topics without local subscribers. The next listen() subscribes such topics again, also when a broker
     * that was shut down after stop() lost them.
     */
    private void settleBrokerState() {
        CompletableFuture<?>[] pending;

        synchronized (sessionTopicLock) {
            pending = topicBrokerOperations.values().toArray(CompletableFuture[]::new);
        }

        try {
            CompletableFuture.allOf(pending).get(configuration.getCloseTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException exception) {
            LOGGER.warn("Topic broker operations did not complete within the close timeout");
        } catch (ExecutionException exception) {
            // Already logged by the session that started the operation
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }

        synchronized (sessionTopicLock) {
            brokerSubscriptions.entrySet().removeIf(entry ->
                entry.getValue().transition == null && !sessionTopicRegistry.hasSubscribers(entry.getKey()));
        }
    }

    /**
     * Sends a going away close frame to every connected session and waits until the clients
     * answer it or the close timeout elapses, then closes the connections that are still open.
     */
    private void closeSessions() {
        List<ChannelFuture> closeFutures = new ArrayList<>();
        Duration closeTimeout = configuration.getCloseTimeout();

        for (WebSocketSession<T, D> session : sessionRegistry.snapshot()) {
            Channel channel = session.getChannelContext().channel();
            CloseWebSocketFrame closeFrame = new CloseWebSocketFrame(WebSocketCloseStatus.ENDPOINT_UNAVAILABLE);

            closeFutures.add(ClosingHandshake.start(channel, closeFrame, closeTimeout));
        }

        long deadline = System.nanoTime() + closeTimeout.toNanos();

        for (ChannelFuture closeFuture : closeFutures) {
            Channel channel = closeFuture.channel();
            long remaining = Math.max(0, deadline - System.nanoTime());

            // The answer of a client served by the calling thread could only be read after stop() returns
            if (channel.eventLoop().inEventLoop() || !closeFuture.awaitUninterruptibly(remaining, TimeUnit.NANOSECONDS)) {
                channel.close();
            }
        }
    }

    /**
     * Shuts down the event loop groups and waits for them to terminate, skipping groups that were not created.
     * No quiet period is needed, sessions are already closed and the shutdown closes any remaining connections.
     */
    private static void shutdownGracefully(EventLoopGroup... groups) {
        for (EventLoopGroup group : groups) {
            if (group != null) {
                group.shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        }

        for (EventLoopGroup group : groups) {
            // A callback blocking an event loop would otherwise keep stop() waiting forever
            if (group != null && !group.terminationFuture().awaitUninterruptibly(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOGGER.warn("Event loops did not terminate within {} seconds, a callback may be blocking them", SHUTDOWN_TIMEOUT_SECONDS);
            }
        }
    }

    private static boolean isEventLoopThread(EventLoopGroup... groups) {
        for (EventLoopGroup group : groups) {
            for (EventExecutor executor : group) {
                if (executor.inEventLoop()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Broadcasts a message to all connected WebSocket sessions.
     * While the server is stopping, e.g. from onClose, it is sent to the sessions that are still open.
     *
     * @param message The message to be broadcast
     * @throws IllegalStateException If the server is stopped
     */
    public synchronized void broadcast(T message) {
        if (state == State.STOPPED) {
            throw new IllegalStateException("Server is not running, cannot broadcast!");
        }
        sessionRegistry.snapshot().forEach(session -> session.sendMessage(message));
    }

    /**
     * Returns a snapshot of all currently connected WebSocket sessions.
     * The returned collection is unmodifiable and does not change when sessions connect or disconnect later,
     * but the sessions in it are the live session objects.
     *
     * @return A collection of connected WebSocket sessions
     */
    public Collection<WebSocketSession<T, D>> getConnectedSessions() {
        return sessionRegistry.snapshot();
    }

    /**
     * Checks if the WebSocket server is currently running.
     * Returns false once the server channel has closed, even if stop() was not called yet.
     * Call stop() in that case too, to release the event loops and close the remaining sessions.
     *
     * @return True if the server is running, false otherwise
     */
    public boolean isRunning() {
        return state == State.RUNNING && serverChannel != null;
    }

    /**
     * Returns the address the server is currently bound to. In particular, this exposes the port selected by the
     * operating system when the server was started with {@code listen(0)}.
     *
     * @return The bound local address, or null before the server starts and after its channel closes
     */
    public InetSocketAddress getLocalAddress() {
        Channel channel = serverChannel;
        return channel == null ? null : (InetSocketAddress) channel.localAddress();
    }

    String formatStartupAddress(InetSocketAddress localAddress) {
        String scheme = configuration.getTlsConfiguration() == null ? "ws" : "wss";
        String host = localAddress.getHostString();
        if (host.indexOf(':') >= 0 && !host.startsWith("[")) {
            host = '[' + host + ']';
        }
        return scheme + "://" + host + ':' + localAddress.getPort() + path;
    }

    /**
     * Retrieves a WebSocket session by its channel ID.
     *
     * @param id The channel ID of the session to retrieve
     * @return The WebSocket session associated with the given channel ID, or null if no session exists for the ID
     */
    WebSocketSession<T, D> getSessionByChannelId(ChannelId id) {
        return sessionRegistry.get(id);
    }

    /**
     * Removes a WebSocket session associated with the given channel ID
     * and unsubscribes it from all topics.
     *
     * @param id The channel ID of the session to remove
     */
    void removeSession(ChannelId id) {
        List<BrokerTransition> transitions = new ArrayList<>();

        synchronized (sessionTopicLock) {
            WebSocketSession<T, D> session = sessionRegistry.remove(id);

            if (session != null) {
                for (String topic : sessionTopicRegistry.unsubscribeAll(session)) {
                    transitions.add(prepareBrokerUnsubscribe(topic));
                }
            }
        }

        startAll(transitions).whenComplete((_, failure) -> {
            if (failure != null) {
                LOGGER.warn("Failed to unsubscribe disconnected session from topic broker", failure);
            }
        });
    }

    /**
     * Adds a WebSocket session associated with its channel ID.
     *
     * @param session The WebSocket session to add
     */
    void addSession(WebSocketSession<T, D> session) {
        sessionRegistry.register(session);
    }

    String getPath() {
        return path;
    }

    WebSocketServerConfiguration<T> getConfiguration() {
        return configuration;
    }

    OpenHandler<T, D> getOpenHandler() {
        return openHandler;
    }

    UpgradeHandler<D> getUpgradeHandler() {
        return upgradeHandler;
    }

    MessageHandler<T, D> getMessageHandler() {
        return messageHandler;
    }

    ErrorHandler<T, D> getErrorHandler() {
        return errorHandler;
    }

    CloseHandler<T, D> getCloseHandler() {
        return closeHandler;
    }

    WritabilityHandler<T, D> getWritabilityHandler() {
        return writabilityHandler;
    }

    WebSocketServerObserver<T, D> getObserver() {
        return observer;
    }
}
