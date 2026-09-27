package com.mediadroppy.logging.client.dispatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.mediadroppy.logging.client.support.RecordingPublisher;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AsyncDispatcherTest {

    private final RecordingPublisher publisher = new RecordingPublisher();
    private AsyncDispatcher dispatcher;

    @AfterEach
    void tearDown() {
        if (dispatcher != null) {
            dispatcher.stop(1_000);
        }
    }

    @Test
    void publishesOfferedEventsInOrder() {
        dispatcher = new AsyncDispatcher(publisher, 16, 1, 5);
        dispatcher.start();

        assertThat(dispatcher.offer(utf8("first"))).isTrue();
        assertThat(dispatcher.offer(utf8("second"))).isTrue();
        assertThat(dispatcher.offer(utf8("third"))).isTrue();

        await().atMost(5, SECONDS).until(() -> publisher.published().size() == 3);
        assertThat(publisher.publishedUtf8()).containsExactly("first", "second", "third");
        assertThat(dispatcher.droppedCount()).isZero();
    }

    @Test
    void retriesWithBackoffUntilThePublishSucceeds() {
        publisher.failNext(2);
        dispatcher = new AsyncDispatcher(publisher, 16, 1, 5);
        dispatcher.start();

        dispatcher.offer(utf8("retried"));

        await().atMost(5, SECONDS).until(() -> publisher.published().size() == 1);
        assertThat(publisher.attempts()).isEqualTo(3);
        assertThat(publisher.publishedUtf8()).containsExactly("retried");
        assertThat(dispatcher.droppedCount()).isZero();
    }

    @Test
    void dropsAndCountsWhenTheQueueIsFull() {
        CountDownLatch gate = new CountDownLatch(1);
        publisher.gateOn(gate);
        dispatcher = new AsyncDispatcher(publisher, 1, 1, 5);
        dispatcher.start();

        // First event: taken by the worker, now blocked inside publish.
        assertThat(dispatcher.offer(utf8("in-flight"))).isTrue();
        await().atMost(5, SECONDS).until(() -> publisher.attempts() == 1);
        // Second event: sits in the queue (capacity 1). Third and fourth: no room.
        assertThat(dispatcher.offer(utf8("queued"))).isTrue();
        assertThat(dispatcher.offer(utf8("dropped-1"))).isFalse();
        assertThat(dispatcher.offer(utf8("dropped-2"))).isFalse();
        assertThat(dispatcher.droppedCount()).isEqualTo(2);

        gate.countDown();
        await().atMost(5, SECONDS).until(() -> publisher.published().size() == 2);
        assertThat(publisher.publishedUtf8()).containsExactly("in-flight", "queued");
    }

    @Test
    void stopDrainsWhatIsQueuedBeforeClosing() {
        CountDownLatch gate = new CountDownLatch(1);
        publisher.gateOn(gate);
        dispatcher = new AsyncDispatcher(publisher, 8, 1, 5);
        dispatcher.start();

        dispatcher.offer(utf8("a"));
        dispatcher.offer(utf8("b"));
        dispatcher.offer(utf8("c"));
        await().atMost(5, SECONDS).until(() -> publisher.attempts() >= 1);

        gate.countDown();
        dispatcher.stop(2_000);

        assertThat(publisher.publishedUtf8()).containsExactly("a", "b", "c");
        assertThat(dispatcher.droppedCount()).isZero();
        assertThat(publisher.isClosed()).isTrue();
    }

    @Test
    void stopWithAnUnreachableBrokerReturnsPromptlyAndCountsTheLoss() {
        publisher.failAlways();
        dispatcher = new AsyncDispatcher(publisher, 8, 50, 100);
        dispatcher.start();

        dispatcher.offer(utf8("doomed-1"));
        dispatcher.offer(utf8("doomed-2"));
        await().atMost(5, SECONDS).until(() -> publisher.attempts() >= 1);

        long before = System.nanoTime();
        dispatcher.stop(300);
        long elapsedMillis = (System.nanoTime() - before) / 1_000_000;

        assertThat(elapsedMillis).isLessThan(3_000);
        assertThat(dispatcher.droppedCount()).isEqualTo(2);
        assertThat(publisher.isClosed()).isTrue();
        assertThat(publisher.published()).isEmpty();
    }

    @Test
    void offersAfterStopAreDroppedAndCounted() {
        dispatcher = new AsyncDispatcher(publisher, 8, 1, 5);
        dispatcher.start();
        dispatcher.stop(1_000);

        assertThat(dispatcher.offer(utf8("late"))).isFalse();
        assertThat(dispatcher.droppedCount()).isEqualTo(1);
    }

    @Test
    void publishesFromItsOwnNamedDispatchThread() {
        dispatcher = new AsyncDispatcher(publisher, 8, 1, 5);
        dispatcher.start();

        dispatcher.offer(utf8("thread-check"));

        await().atMost(5, SECONDS).until(() -> publisher.published().size() == 1);
        assertThat(publisher.lastPublishThread().getName()).isEqualTo("md-logging-dispatcher");
        assertThat(publisher.lastPublishThread().isDaemon()).isTrue();
        assertThat(dispatcher.isDispatchThread()).isFalse();
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
