package com.mediadroppy.logging.client.support;

import com.mediadroppy.logging.client.amqp.AmqpLogPublisher;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory {@link AmqpLogPublisher} for unit tests: records bodies, and can be told to fail a
 * number of publishes or to block until a latch opens.
 */
public final class RecordingPublisher implements AmqpLogPublisher {

    private final List<byte[]> published = new CopyOnWriteArrayList<>();
    private final AtomicInteger attempts = new AtomicInteger();
    private final AtomicInteger failuresRemaining = new AtomicInteger();
    private volatile CountDownLatch gate;
    private volatile boolean closed;
    private volatile Thread lastPublishThread;

    /** The next {@code count} publish attempts throw. */
    public void failNext(int count) {
        failuresRemaining.set(count);
    }

    public void failAlways() {
        failuresRemaining.set(Integer.MAX_VALUE);
    }

    /** Every publish attempt blocks until {@code latch} opens. */
    public void gateOn(CountDownLatch latch) {
        this.gate = latch;
    }

    @Override
    public void publish(byte[] body) throws Exception {
        lastPublishThread = Thread.currentThread();
        attempts.incrementAndGet();
        CountDownLatch currentGate = gate;
        if (currentGate != null) {
            currentGate.await();
        }
        if (failuresRemaining.get() > 0) {
            failuresRemaining.decrementAndGet();
            throw new IOException("simulated publish failure");
        }
        published.add(body);
    }

    @Override
    public void close() {
        closed = true;
    }

    public List<byte[]> published() {
        return published;
    }

    public List<String> publishedUtf8() {
        return published.stream().map(body -> new String(body, StandardCharsets.UTF_8)).toList();
    }

    public int attempts() {
        return attempts.get();
    }

    public boolean isClosed() {
        return closed;
    }

    public Thread lastPublishThread() {
        return lastPublishThread;
    }
}
