package com.mediadroppy.logging.client.amqp;

/**
 * Connection and topology settings for publishing to the logging exchange.
 *
 * <p>Defaults mirror {@code md-logging-agent}: exchange {@code logging-events}, a durable topic
 * exchange. The agent binds its queue with routing key {@code #} by default, so any routing key
 * reaches it; using the application name gives other consumers something to filter on.
 */
public record AmqpPublisherConfig(
        String host,
        int port,
        String virtualHost,
        String username,
        String password,
        String exchange,
        String routingKey,
        boolean declareExchange,
        int connectionTimeoutMillis,
        String connectionName) {

    public AmqpPublisherConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be in [1, 65535], got " + port);
        }
        if (exchange == null || exchange.isBlank()) {
            throw new IllegalArgumentException("exchange must not be blank");
        }
        if (routingKey == null) {
            throw new IllegalArgumentException("routingKey must not be null");
        }
        if (connectionTimeoutMillis <= 0) {
            throw new IllegalArgumentException("connectionTimeoutMillis must be positive");
        }
    }

    /** Credentials stay out of appender status output and thread dumps. */
    @Override
    public String toString() {
        return "AmqpPublisherConfig[host=" + host + ", port=" + port + ", virtualHost=" + virtualHost
                + ", username=" + username + ", password=*****, exchange=" + exchange
                + ", routingKey=" + routingKey + ", declareExchange=" + declareExchange
                + ", connectionTimeoutMillis=" + connectionTimeoutMillis
                + ", connectionName=" + connectionName + "]";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String host = "localhost";
        private int port = 5672;
        private String virtualHost = "/";
        private String username = "guest";
        private String password = "guest";
        private String exchange = "logging-events";
        private String routingKey = "";
        private boolean declareExchange = true;
        private int connectionTimeoutMillis = 5_000;
        private String connectionName = "md-logging-java";

        private Builder() {
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder virtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder exchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder routingKey(String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

        public Builder declareExchange(boolean declareExchange) {
            this.declareExchange = declareExchange;
            return this;
        }

        public Builder connectionTimeoutMillis(int connectionTimeoutMillis) {
            this.connectionTimeoutMillis = connectionTimeoutMillis;
            return this;
        }

        public Builder connectionName(String connectionName) {
            this.connectionName = connectionName;
            return this;
        }

        public AmqpPublisherConfig build() {
            return new AmqpPublisherConfig(host, port, virtualHost, username, password, exchange,
                    routingKey, declareExchange, connectionTimeoutMillis, connectionName);
        }
    }
}
