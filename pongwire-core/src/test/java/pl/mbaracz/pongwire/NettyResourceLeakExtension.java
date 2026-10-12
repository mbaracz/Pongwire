package pl.mbaracz.pongwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.ref.WeakReference;

/**
 * Releases test channels and turns asynchronous Netty leak reports into test failures.
 */
public final class NettyResourceLeakExtension implements AfterEachCallback {

    private static final int GC_ATTEMPTS = 10;

    @Override
    public void afterEach(ExtensionContext context) throws InterruptedException {
        Util.finishAndReleaseEmbeddedChannels();
        awaitGarbageCollection();

        // A new tracked allocation makes Netty inspect the reference queue for earlier leaks.
        ByteBuf trigger = Unpooled.buffer(1);
        trigger.release();

        FailingResourceLeakDetector.assertNoLeaks();
    }

    private static void awaitGarbageCollection() throws InterruptedException {
        Object marker = new Object();
        WeakReference<Object> reference = new WeakReference<>(marker);
        marker = null;

        for (int attempt = 0; attempt < GC_ATTEMPTS && reference.get() != null; attempt++) {
            System.gc();
            Thread.sleep(10);
        }

        if (reference.get() != null) {
            throw new AssertionError("Could not force garbage collection for Netty leak detection");
        }
    }
}
