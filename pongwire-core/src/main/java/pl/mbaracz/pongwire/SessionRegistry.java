package pl.mbaracz.pongwire;

import io.netty.channel.ChannelId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps the sessions that completed their WebSocket handshake and have not disconnected yet.
 * Registration and the hand-over of the sessions to close on stop() exclude each other, so a session is either
 * in that snapshot or refused.
 */
final class SessionRegistry<T, D> {

    private final Map<ChannelId, WebSocketSession<T, D>> sessions = new ConcurrentHashMap<>();
    private boolean accepting = true;

    /**
     * Registers a session of the running server.
     *
     * @param session the session that completed its handshake.
     * @return false if the server is not accepting sessions, i.e. stop() already took the sessions to close.
     */
    synchronized boolean register(WebSocketSession<T, D> session) {
        if (!accepting) {
            return false;
        }

        sessions.put(session.getChannelContext().channel().id(), session);
        return true;
    }

    synchronized void startAccepting() {
        accepting = true;
    }

    /**
     * Stops accepting sessions and returns the registered ones, which the server closes.
     *
     * @return the sessions registered so far.
     */
    synchronized Collection<WebSocketSession<T, D>> stopAccepting() {
        accepting = false;
        return snapshot();
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
