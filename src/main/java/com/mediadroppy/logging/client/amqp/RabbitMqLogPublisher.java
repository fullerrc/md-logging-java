package com.mediadroppy.logging.client.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Lazily connecting RabbitMQ publisher.
 *
 * <p>No connection is opened until the first publish, so an appender can be configured and started
 * while the broker is still down — events buffer in the dispatcher and flow once the broker
 * appears. Automatic recovery is disabled: the dispatcher already retries with backoff, and a
 * second, library-internal recovery loop underneath it would fight over the same connection.
 *
 * <p>On connect the publisher declares the exchange exactly as the agent does — durable topic,
 * not auto-delete — so whichever side starts first creates it and the other's declaration is a
 * no-op instead of a channel error.
 */
public final class RabbitMqLogPublisher implements AmqpLogPublisher {

    private static final AMQP.BasicProperties PROPERTIES = new AMQP.BasicProperties.Builder()
            .contentType("application/json")
            .deliveryMode(2) // persistent: survives a broker restart, like the durable exchange
            .build();

    private final AmqpPublisherConfig config;
    private final ConnectionFactory factory;

    private Connection connection;
    private Channel channel;

    public RabbitMqLogPublisher(AmqpPublisherConfig config) {
        this(config, defaultFactory(config));
    }

    RabbitMqLogPublisher(AmqpPublisherConfig config, ConnectionFactory factory) {
        this.config = config;
        this.factory = factory;
    }

    static ConnectionFactory defaultFactory(AmqpPublisherConfig config) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setVirtualHost(config.virtualHost());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        // The default is 60s; a down broker must not stall the dispatch thread for a minute
        // per attempt when the dispatcher's own backoff is doing the pacing.
        factory.setConnectionTimeout(config.connectionTimeoutMillis());
        factory.setAutomaticRecoveryEnabled(false);
        return factory;
    }

    @Override
    public synchronized void publish(byte[] body) throws IOException, TimeoutException {
        ensureChannel();
        try {
            channel.basicPublish(config.exchange(), config.routingKey(), PROPERTIES, body);
        } catch (IOException | ShutdownSignalException e) {
            // Discard the broken channel so the next attempt reconnects from scratch.
            closeQuietly();
            throw e;
        }
    }

    private void ensureChannel() throws IOException, TimeoutException {
        if (channel != null && channel.isOpen()) {
            return;
        }
        closeQuietly();
        Connection candidate = factory.newConnection(config.connectionName());
        try {
            Channel created = candidate.createChannel();
            if (created == null) {
                throw new IOException("Broker refused to allocate a channel");
            }
            if (config.declareExchange()) {
                created.exchangeDeclare(config.exchange(), "topic", true, false, null);
            }
            connection = candidate;
            channel = created;
        } catch (IOException | RuntimeException e) {
            abortQuietly(candidate);
            throw e;
        }
    }

    @Override
    public synchronized void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        if (connection != null) {
            abortQuietly(connection);
        }
        connection = null;
        channel = null;
    }

    private void abortQuietly(Connection target) {
        try {
            // abort, not close: close throws on an already-broken connection, abort never does.
            target.abort(1_000);
        } catch (RuntimeException ignored) {
            // Nothing sensible to do with a failure to abandon a connection.
        }
    }
}
