package pl.mbaracz.pongwire;

import javax.net.ssl.SSLSession;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Metadata from an HTTP request to upgrade a connection to WebSocket.
 * All collections returned by this class are immutable.
 */
public final class UpgradeRequest {

    private final String path;
    private final Map<String, List<String>> queryParameters;
    private final Map<String, String> cookies;
    private final Map<String, List<String>> headers;
    private final InetSocketAddress remoteAddress;
    private final SSLSession sslSession;

    UpgradeRequest(
        String path,
        Map<String, List<String>> queryParameters,
        Map<String, String> cookies,
        Map<String, List<String>> headers,
        InetSocketAddress remoteAddress,
        SSLSession sslSession
    ) {
        this.path = path;
        this.queryParameters = immutableMultiValueMap(queryParameters, false);
        this.cookies = Collections.unmodifiableMap(new LinkedHashMap<>(cookies));
        this.headers = immutableMultiValueMap(headers, true);
        this.remoteAddress = remoteAddress;
        this.sslSession = sslSession;
    }

    public String getPath() {
        return path;
    }

    public Map<String, List<String>> getQueryParameters() {
        return queryParameters;
    }

    /**
     * Returns the first value of the query parameter.
     *
     * @return the first value, or {@code null} if the parameter is absent or has no values.
     */
    public String getQueryParameter(String name) {
        List<String> values = queryParameters.get(name);
        return values == null || values.isEmpty() ? null : values.getFirst();
    }

    public Map<String, String> getCookies() {
        return cookies;
    }

    /**
     * Returns the value of the named cookie.
     * Cookies are parsed leniently, like browsers send them, also values that are not strict RFC 6265.
     * The raw header is available through {@code getHeader("Cookie")}.
     *
     * @return the cookie value, or {@code null} if the cookie is absent.
     */
    public String getCookie(String name) {
        return cookies.get(name);
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    /**
     * Returns the first value for the header name, matched case-insensitively.
     *
     * @return the first value, or {@code null} if the header is absent or has no values.
     */
    public String getHeader(String name) {
        List<String> values = headers.get(name);
        return values == null || values.isEmpty() ? null : values.getFirst();
    }

    /**
     * Returns the remote network address of the connection.
     *
     * @return the remote address, or {@code null} if the channel does not expose an
     *         {@link InetSocketAddress}.
     */
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * Returns the TLS session of the connection, e.g. to read the client certificate with
     * {@code getPeerCertificates()} when the TLS context requires client authentication.
     *
     * @return the TLS session, or {@code null} without TLS.
     */
    public SSLSession getSslSession() {
        return sslSession;
    }

    private static Map<String, List<String>> immutableMultiValueMap(
        Map<String, List<String>> source,
        boolean caseInsensitive
    ) {
        Map<String, List<String>> copy = caseInsensitive
            ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER)
            : new LinkedHashMap<>();

        source.forEach((name, values) -> copy
            .computeIfAbsent(name, _ -> new ArrayList<>())
            .addAll(values));

        copy.replaceAll((_, values) -> List.copyOf(values));

        return Collections.unmodifiableMap(copy);
    }
}
