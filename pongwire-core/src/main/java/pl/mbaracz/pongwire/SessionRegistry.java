package pl.mbaracz.pongwire;

import io.netty.channel.ChannelId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps the sessions that completed their WebSocket handshake and have not disconnected yet.
 */
final class SessionRegistry<T, D> {

    private final Map<ChannelId, WebSocketSession<T, D>> sessions = new ConcurrentHashMap<>();

    void register(WebSocketSession<T, D> session) {
        sessions.put(session.getChannelContext().channel().id(), session);
    }

    WebSocketSession<T, D> remove(ChannelId id) {
        return sessions.remove(id);
    }

    WebSocketSession<T, D> get(ChannelId id) {
        return sessions.get(id);
    }

    boolean contains(WebSocketSession<T, D> session) {
        if (session == null || session.getChannelContext() == null) {
            return false;
        }

        return sessions.get(session.getChannelContext().channel().id()) == session;
    }

    Collection<WebSocketSession<T, D>> snapshot() {
        return List.copyOf(sessions.values());
    }

    int size() {
        return sessions.size();
    }
}
