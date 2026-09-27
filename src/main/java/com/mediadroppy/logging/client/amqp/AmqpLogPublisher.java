package com.mediadroppy.logging.client.amqp;

/**
 * Publishes one encoded envelope. Implementations may block (connecting, broker flow control) —
 * callers are expected to invoke this from a dedicated dispatch thread, never an application
 * thread.
 */
public interface AmqpLogPublisher extends AutoCloseable {

    /**
     * @throws Exception on any failure; the caller owns retry policy. Implementations should
     *         discard broken connection state before throwing so the next call can reconnect.
     */
    void publish(byte[] body) throws Exception;

    /** Never throws; a logging pipeline must not fail an application shutdown. */
    @Override
    void close();
}
