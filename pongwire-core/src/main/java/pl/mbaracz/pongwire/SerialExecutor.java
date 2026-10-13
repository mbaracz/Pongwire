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
 * A task must not throw, the exception escapes to the executor and the tasks after it stay queued.
 */
final class SerialExecutor {

    private record Entry(Runnable task, Consumer<RejectedExecutionException> onRejected) {
    }

    private final Queue<Entry> tasks = new ArrayDeque<>();
    private final Executor executor;
    private boolean active;

    // The thread handing the tasks to the executor, so a task the executor runs on that thread
    // does not hand over the next one recursively, which would grow the stack with the queue
    private Thread draining;

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
     * Hands the queued tasks to the executor, one after the previous one finished.
     * Called outside the monitor, so an executor running tasks on the calling
     * thread does not block the submission of further tasks.
     */
    private void scheduleNext() {
        Thread thread = Thread.currentThread();

        synchronized (this) {
            // A task that ran on this thread returned to the loop below, which continues with the next one
            if (draining == thread) {
                return;
            }
            draining = thread;
        }

        try {
            while (true) {
                Entry next;

                synchronized (this) {
                    next = tasks.poll();

                    if (next == null) {
                        active = false;
                        return;
                    }
                }

                Thread[] ranOn = new Thread[1];

                try {
                    executor.execute(() -> {
                        ranOn[0] = Thread.currentThread();
                        try {
                            next.task().run();
                        } finally {
                            scheduleNext();
                        }
                    });
                } catch (RuntimeException exception) {
                    // A task that ran on this thread and threw is not a rejected one
                    if (ranOn[0] == thread) {
                        throw exception;
                    }

                    // The rejected task never runs, the next one is tried
                    next.onRejected().accept(exception instanceof RejectedExecutionException rejection
                        ? rejection
                        : new RejectedExecutionException(exception));
                    continue;
                }

                // A task running on another thread hands over the next one when it finishes
                if (ranOn[0] != thread) {
                    return;
                }
            }
        } finally {
            synchronized (this) {
                if (draining == thread) {
                    draining = null;
                }
            }
        }
    }
}
