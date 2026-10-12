package pl.mbaracz.pongwire;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * HTTP response sent when an upgrade is rejected.
 */
public final class UpgradeResponse {

    private int status = 400;
    private final Map<String, List<String>> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public UpgradeResponse setStatus(int statusCode) {
        if (statusCode < 100 || statusCode > 599) {
            throw new IllegalArgumentException("HTTP status code must be between 100 and 599");
        }
        this.status = statusCode;
        return this;
    }

    public UpgradeResponse setHeader(String name, String value) {
        List<String> values = new ArrayList<>();
        values.add(value);
        headers.put(name, values);
        return this;
    }

    public UpgradeResponse addHeader(String name, String value) {
        headers.computeIfAbsent(name, _ -> new ArrayList<>()).add(value);
        return this;
    }

    public int getStatus() {
        return status;
    }

    Map<String, List<String>> getHeaders() {
        return Collections.unmodifiableMap(headers);
    }
}
