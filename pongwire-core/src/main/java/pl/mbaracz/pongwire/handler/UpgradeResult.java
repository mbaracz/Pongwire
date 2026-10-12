package pl.mbaracz.pongwire.handler;

/**
 * Result of an upgrade request: accepted with the context of the new session, or rejected.
 *
 * @param <D> the type of the session context.
 */
public final class UpgradeResult<D> {

    private final boolean accepted;
    private final D context;

    private UpgradeResult(boolean accepted, D context) {
        this.accepted = accepted;
        this.context = context;
    }

    /**
     * Accepts the upgrade and creates the session with the given context.
     *
     * @param context the context of the new session, may be null.
     * @param <D>     the type of the session context.
     * @return the accepting result.
     */
    public static <D> UpgradeResult<D> accept(D context) {
        return new UpgradeResult<>(true, context);
    }

    /**
     * Rejects the upgrade, so the response given to the upgrade handler is sent to the client.
     *
     * @param <D> the type of the session context.
     * @return the rejecting result.
     */
    public static <D> UpgradeResult<D> reject() {
        return new UpgradeResult<>(false, null);
    }

    public boolean isAccepted() {
        return accepted;
    }

    public D getContext() {
        return context;
    }
}
