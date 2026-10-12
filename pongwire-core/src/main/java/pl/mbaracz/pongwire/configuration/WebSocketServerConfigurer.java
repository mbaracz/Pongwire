package pl.mbaracz.pongwire.configuration;

/**
 * Interface for configuring WebSocket server settings.
 *
 * @param <T> the type of messages that will be handled by the WebSocket server.
 */
@FunctionalInterface
public interface WebSocketServerConfigurer<T> {

    /**
     * Configures the WebSocket server settings.
     *
     * @param configurer the builder of the WebSocket server configuration, starting from the current settings.
     */
    void configure(WebSocketServerConfiguration.Builder<T> configurer);

}
