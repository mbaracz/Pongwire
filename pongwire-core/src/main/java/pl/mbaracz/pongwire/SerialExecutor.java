package pl.mbaracz.pongwire;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

/**
 * Runs tasks on another executor one at a time, in the order they were submitted.
 * Every task either runs or is passed to its rejection handler, also when the executor
 * rejects it only after the earlier tasks have run.
 */
final class SerialExecutor {

    private record Entry(Runnable task, Consumer<RejectedExecutionException> onRejected) {
    }

    private final Queue<Entry> tasks = new ArrayDeque<>();
    private final Executor executor;
    private boolean active;

    SerialExecutor(Executor executor) {
        this.executor = executor;
    }

    /**
     * Submits a task to run after the earlier ones.
     *
     * @param task       the task.
     * @param onRejected called with the rejection if the executor does not accept the task, on the submitting thread
     *                   or on the thread of the previous task.
     */
    void execute(Runnable task, Consumer<RejectedExecutionException> onRejected) {
        synchronized (this) {
            tasks.add(new Entry(task, onRejected));

            if (active) {
                return;
            }
            active = true;
        }

        scheduleNext();
    }

    /**
     * Hands the next task to the executor.
     * Called outside the monitor, so an executor running tasks on the calling
     * thread does not block the submission of further tasks.
     */
    private void scheduleNext() {
        while (true) {
            Entry next;

            synchronized (this) {
                next = tasks.poll();

                if (next == null) {
                    active = false;
                    return;
                }
            }

            try {
                executor.execute(() -> {
                    try {
                        next.task().run();
                    } finally {
                        scheduleNext();
                    }
                });
                return;
            } catch (RuntimeException exception) {
                // The rejected task never runs, the next one is tried
                next.onRejected().accept(exception instanceof RejectedExecutionException rejection
                    ? rejection
                    : new RejectedExecutionException(exception));
            }
        }
    }
}
