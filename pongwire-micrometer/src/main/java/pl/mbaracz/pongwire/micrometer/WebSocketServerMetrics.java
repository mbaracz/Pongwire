package pl.mbaracz.pongwire.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import pl.mbaracz.pongwire.WebSocketServerObserver;
import pl.mbaracz.pongwire.WebSocketSession;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Records the sessions, messages, errors and rejected handshakes of a WebSocket server in a Micrometer registry.
 * Closed sessions are tagged with their close code and rejected handshakes with their HTTP status.
 *
 * @param <T> the type of the WebSocket message.
 * @param <D> the type of additional data associated with the WebSocket session.
 */
public class WebSocketServerMetrics<T, D> implements WebSocketServerObserver<T, D> {

    private final AtomicInteger activeSessions = new AtomicInteger();
    private final MeterRegistry registry;
    // Copied once, so the meters created later carry the same tags as the ones registered here,
    // also when the caller changes its collection afterwards
    private final Tags tags;
    private final Counter sessionsOpened;
    private final Counter messagesReceived;
    private final Counter messagesSent;
    private final Counter errors;

    /**
     * Registers the server metrics in the given registry.
     *
     * @param registry the registry to record the metrics in.
     */
    public WebSocketServerMetrics(MeterRegistry registry) {
        this(registry, Tags.empty());
    }

    /**
     * Registers the server metrics with the given tags in the given registry.
     *
     * @param registry the registry to record the metrics in.
     * @param tags     the tags added to every metric, e.g. to tell several servers apart.
     */
    public WebSocketServerMetrics(MeterRegistry registry, Iterable<Tag> tags) {
        this.registry = registry;
        this.tags = Tags.of(tags);

        Gauge.builder("pongwire.sessions.active", activeSessions, AtomicInteger::get)
            .description("The number of open sessions")
            .tags(tags)
            .register(registry);

        this.sessionsOpened = counter(registry, tags, "pongwire.sessions.opened", "The number of opened sessions");
        this.messagesReceived = counter(registry, tags, "pongwire.messages.received", "The number of received messages");
        this.messagesSent = counter(registry, tags, "pongwire.messages.sent", "The number of messages successfully written to connections");
        this.errors = counter(registry, tags, "pongwire.errors", "The number of exceptions caught on connections");
    }

    private static Counter counter(MeterRegistry registry, Iterable<Tag> tags, String name, String description) {
        return Counter.builder(name)
            .description(description)
            .tags(tags)
            .register(registry);
    }

    @Override
    public void sessionOpened(WebSocketSession<T, D> session) {
        activeSessions.incrementAndGet();
        sessionsOpened.increment();
    }

    @Override
    public void sessionClosed(WebSocketSession<T, D> session, int code, String reason) {
        activeSessions.decrementAndGet();
        counter(registry, tags.and("code", String.valueOf(code)), "pongwire.sessions.closed", "The number of closed sessions")
            .increment();
    }

    @Override
    public void handshakeRejected(int status) {
        counter(registry, tags.and("status", String.valueOf(status)), "pongwire.handshakes.rejected", "The number of rejected handshakes")
            .increment();
    }

    @Override
    public void messageReceived(WebSocketSession<T, D> session) {
        messagesReceived.increment();
    }

    @Override
    public void messageSent(WebSocketSession<T, D> session) {
        messagesSent.increment();
    }

    @Override
    public void exception(WebSocketSession<T, D> session, Throwable exception) {
        errors.increment();
    }
}
