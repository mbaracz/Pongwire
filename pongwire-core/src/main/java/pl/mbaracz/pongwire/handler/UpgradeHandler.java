package pl.mbaracz.pongwire.handler;

import pl.mbaracz.pongwire.UpgradeRequest;
import pl.mbaracz.pongwire.UpgradeResponse;

/**
 * Interface for handling the upgrade of an HTTP connection to a WebSocket connection before the handshake.
 *
 * @param <D> the type of the session context.
 */
@FunctionalInterface
public interface UpgradeHandler<D> {

    /**
     * Handles custom processing of the upgrade request before the WebSocket handshake.
     * If the upgrade is rejected, the provided HTTP response will be sent.
     *
     * @param request  metadata from the HTTP request initiating the upgrade.
     * @param response the HTTP response to be sent if the upgrade is rejected, 400 (bad request) by default.
     * @return {@link UpgradeResult#accept(Object)} with the context of the new session, or {@link UpgradeResult#reject()}.
     */
    UpgradeResult<D> handleUpgrade(UpgradeRequest request, UpgradeResponse response);

}
