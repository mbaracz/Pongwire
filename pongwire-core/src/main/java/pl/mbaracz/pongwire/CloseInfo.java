package pl.mbaracz.pongwire;

import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.util.AttributeKey;

/**
 * Close status reported to the close handler of a session.
 * The server stores it on the channel when it closes the connection itself,
 * so that closure is not reported as abnormal.
 *
 * @param code   the close status code.
 * @param reason the close reason.
 */
record CloseInfo(int code, String reason) {

    static final AttributeKey<CloseInfo> KEY = AttributeKey.valueOf(CloseInfo.class, "closeInfo");

    static CloseInfo of(WebSocketCloseStatus status) {
        return new CloseInfo(status.code(), status.reasonText());
    }
}
