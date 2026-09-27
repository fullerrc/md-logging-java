package com.mediadroppy.logging.client.dispatch;

import com.mediadroppy.logging.client.amqp.AmqpLogPublisher;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.status.StatusLogger;

/**
 * Decouples application threads from the broker: {@link #offer} never blocks and never throws.
 *
 * <p>Events queue into a bounded buffer drained by a single daemon thread that publishes with
 * exponential-backoff retry. When the buffer is full — broker down long enough, or the
 * application out-logging the link — new events are <em>dropped and counted</em>, because the
 * alternatives are blocking the application (a logging library must never do that) or unbounded
 * memory growth (which turns a broker outage into an application outage).
 *
 * <p>All internal reporting goes through Log4j's {@link StatusLogger}, never through a Log4j
 * logger: this code runs underneath the appender, and logging through the pipeline it implements
 * would feed it its own failure reports.
 */
public final class AsyncDispatcher {

    private static final Logger STATUS = StatusLogger.getLogger();
    private static final long POLL_MILLIS = 100;

    private final AmqpLogPublisher publisher;
    private final BlockingQueue<byte[]> queue;
    private final long initialBackoffMillis;
    private final long maxBackoffMillis;
    private final AtomicLong dropped = new AtomicLong();
    private final Thread worker;

    private volatile boolean running = true;
    /** Only ever touched from the worker thread. */
    private boolean failing;

    public AsyncDispatcher(AmqpLogPublisher publisher, int capacity,
                           long initialBackoffMillis, long maxBackoffMillis) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.publisher = publisher;
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.initialBackoffMillis = Math.max(1, initialBackoffMillis);
        this.maxBackoffMillis = Math.max(this.initialBackoffMillis, maxBackoffMillis);
        this.worker = new Thread(this::drainLoop, "md-logging-dispatcher");
        this.worker.setDaemon(true);
    }

    public void start() {
        worker.start();
    }

    /** @return false when the event was dropped (queue full or dispatcher stopped) */
    public boolean offer(byte[] body) {
        if (running && queue.offer(body)) {
            return true;
        }
        long total = dropped.incrementAndGet();
        if (total == 1 || total % 1000 == 0) {
            STATUS.warn("md-logging-java has dropped {} log event(s); broker unreachable or queue full", total);
        }
        return false;
    }

    /** Events lost to a full queue, a failed shutdown flush, or offers after stop. */
    public long droppedCount() {
        return dropped.get();
    }

    /** True on the dispatch thread itself — used to keep the pipeline from consuming its own output. */
    public boolean isDispatchThread() {
        return Thread.currentThread() == worker;
    }

    /**
     * Stops accepting events, gives the worker {@code timeoutMillis} to drain what is queued, then
     * abandons the rest (counted as dropped) and closes the publisher. Never throws.
     */
    public void stop(long timeoutMillis) {
        running = false;
        try {
            worker.join(Math.max(1, timeoutMillis));
            if (worker.isAlive()) {
                worker.interrupt();
                worker.join(1_000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        ArrayList<byte[]> abandoned = new ArrayList<>();
        queue.drainTo(abandoned);
        if (!abandoned.isEmpty()) {
            long total = dropped.addAndGet(abandoned.size());
            STATUS.warn("md-logging-java dropped {} unflushed log event(s) at shutdown ({} total)",
                    abandoned.size(), total);
        }
        publisher.close();
    }

    private void drainLoop() {
        while (true) {
            byte[] next;
            try {
                next = queue.poll(POLL_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return; // stop() interrupting an overrunning worker
            }
            if (next == null) {
                if (!running) {
                    return; // graceful stop: queue fully drained
                }
                continue;
            }
            if (!publishWithRetry(next)) {
                return;
            }
        }
    }

    /** @return false only when shutting down, after counting the event as dropped */
    private boolean publishWithRetry(byte[] body) {
        long backoff = initialBackoffMillis;
        while (true) {
            try {
                publisher.publish(body);
                if (failing) {
                    failing = false;
                    STATUS.info("md-logging-java reconnected to the logging broker");
                }
                return true;
            } catch (Exception e) {
                if (!failing) {
                    failing = true;
                    STATUS.warn("md-logging-java failed to publish a log event; retrying with backoff", e);
                }
                if (!running) {
                    dropped.incrementAndGet();
                    return false;
                }
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    dropped.incrementAndGet();
                    return false;
                }
                backoff = Math.min(backoff * 2, maxBackoffMillis);
            }
        }
    }
}
