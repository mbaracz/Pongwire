package pl.mbaracz.pongwire;

import io.netty.util.ResourceLeakDetector;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Makes Netty leak reports observable by the JUnit leak-detection extension.
 */
public final class FailingResourceLeakDetector<T> extends ResourceLeakDetector<T> {

    private static final AtomicReference<AssertionError> DETECTED_LEAK = new AtomicReference<>();

    public FailingResourceLeakDetector(Class<?> resourceType, int samplingInterval) {
        super(resourceType, samplingInterval);
    }

    @Deprecated
    public FailingResourceLeakDetector(Class<?> resourceType, int samplingInterval, long maxActive) {
        super(resourceType, samplingInterval, maxActive);
    }

    @Override
    protected void reportTracedLeak(String resourceType, String records) {
        super.reportTracedLeak(resourceType, records);
        DETECTED_LEAK.compareAndSet(null, new AssertionError("Netty resource leak detected: " + resourceType + records));
    }

    @Override
    protected void reportUntracedLeak(String resourceType) {
        super.reportUntracedLeak(resourceType);
        DETECTED_LEAK.compareAndSet(null, new AssertionError("Netty resource leak detected: " + resourceType));
    }

    static void assertNoLeaks() {
        AssertionError leak = DETECTED_LEAK.getAndSet(null);

        if (leak != null) {
            throw leak;
        }
    }
}
