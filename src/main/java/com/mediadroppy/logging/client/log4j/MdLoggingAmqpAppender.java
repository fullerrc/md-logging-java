package com.mediadroppy.logging.client.log4j;

import com.mediadroppy.logging.client.amqp.AmqpLogPublisher;
import com.mediadroppy.logging.client.amqp.AmqpPublisherConfig;
import com.mediadroppy.logging.client.amqp.RabbitMqLogPublisher;
import com.mediadroppy.logging.client.dispatch.AsyncDispatcher;
import com.mediadroppy.logging.client.envelope.LogEnvelopeEncoder;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.status.StatusLogger;

/**
 * Log4j 2 appender publishing {@code md-logging-agent} envelopes to an AMQP topic exchange.
 *
 * <p>XML configuration:
 *
 * <pre>{@code
 * <MdLoggingAmqp name="mdLogging" application="md-user"
 *                host="${env:RABBIT_HOST}" port="${env:RABBIT_PORT}"
 *                username="${env:MD_USER_RABBIT_USERNAME}"
 *                password="${env:MD_USER_RABBIT_PASSWORD}"/>
 * }</pre>
 *
 * <p>Only {@code name} and {@code application} are required; everything else defaults to the
 * values the agent's local profile uses. {@code podName} defaults to the {@code HOSTNAME}
 * environment variable, which Kubernetes sets to the pod name.
 *
 * <p>Appending never blocks and never throws into the application (unless
 * {@code ignoreExceptions="false"}): events are handed to a bounded queue drained by a background
 * thread, and are dropped — with a counted, status-logged warning — when the broker stays
 * unreachable long enough to fill it.
 */
@Plugin(name = MdLoggingAmqpAppender.PLUGIN_NAME, category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class MdLoggingAmqpAppender extends AbstractAppender {

    public static final String PLUGIN_NAME = "MdLoggingAmqp";

    private final LogEnvelopeEncoder encoder;
    private final LogEventConverter converter;
    private final AsyncDispatcher dispatcher;
    private final long shutdownTimeoutMillis;

    private MdLoggingAmqpAppender(String name, Filter filter, boolean ignoreExceptions,
                                  Property[] properties, LogEnvelopeEncoder encoder,
                                  LogEventConverter converter, AsyncDispatcher dispatcher,
                                  long shutdownTimeoutMillis) {
        super(name, filter, null, ignoreExceptions, properties);
        this.encoder = encoder;
        this.converter = converter;
        this.dispatcher = dispatcher;
        this.shutdownTimeoutMillis = shutdownTimeoutMillis;
    }

    @Override
    public void start() {
        dispatcher.start();
        super.start();
    }

    @Override
    public void append(LogEvent event) {
        // If the AMQP client library's own logging is routed into log4j and reaches this appender,
        // events raised on the dispatch thread would feed the pipeline its own connection errors.
        if (dispatcher.isDispatchThread()) {
            return;
        }
        try {
            byte[] body = encoder.encode(event.getTimeMillis(), converter.toLogEntry(event));
            dispatcher.offer(body);
        } catch (Exception e) {
            error("Unable to encode log event for AMQP publication", event, e);
            if (!ignoreExceptions()) {
                throw new AppenderLoggingException(e);
            }
        }
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        setStopping();
        // The configured flush window, not log4j's stop timeout: context shutdown passes 0.
        dispatcher.stop(shutdownTimeoutMillis);
        return super.stop(timeout, timeUnit);
    }

    /** Events dropped instead of published; exposed for tests and health reporting. */
    public long droppedCount() {
        return dispatcher.droppedCount();
    }

    @PluginBuilderFactory
    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder extends AbstractAppender.Builder<Builder>
            implements org.apache.logging.log4j.core.util.Builder<MdLoggingAmqpAppender> {

        @PluginBuilderAttribute
        private String application;

        @PluginBuilderAttribute
        private String podName;

        @PluginBuilderAttribute
        private String host = "localhost";

        @PluginBuilderAttribute
        private int port = 5672;

        @PluginBuilderAttribute
        private String virtualHost = "/";

        @PluginBuilderAttribute
        private String username = "guest";

        @PluginBuilderAttribute(sensitive = true)
        private String password = "guest";

        @PluginBuilderAttribute
        private String exchange = "logging-events";

        /** Defaults to the application name; the agent binds {@code #} so anything reaches it. */
        @PluginBuilderAttribute
        private String routingKey;

        @PluginBuilderAttribute
        private boolean declareExchange = true;

        @PluginBuilderAttribute
        private int queueCapacity = 8_192;

        @PluginBuilderAttribute
        private int maxEnvelopeBytes = LogEnvelopeEncoder.DEFAULT_MAX_ENVELOPE_BYTES;

        @PluginBuilderAttribute
        private int connectionTimeoutMillis = 5_000;

        @PluginBuilderAttribute
        private long shutdownTimeoutMillis = 2_000;

        /** Test seam; production always uses the RabbitMQ publisher. */
        private Function<AmqpPublisherConfig, AmqpLogPublisher> publisherFactory = RabbitMqLogPublisher::new;

        public Builder setApplication(String application) {
            this.application = application;
            return this;
        }

        public Builder setPodName(String podName) {
            this.podName = podName;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setExchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder setRoutingKey(String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

        public Builder setDeclareExchange(boolean declareExchange) {
            this.declareExchange = declareExchange;
            return this;
        }

        public Builder setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return this;
        }

        public Builder setMaxEnvelopeBytes(int maxEnvelopeBytes) {
            this.maxEnvelopeBytes = maxEnvelopeBytes;
            return this;
        }

        public Builder setConnectionTimeoutMillis(int connectionTimeoutMillis) {
            this.connectionTimeoutMillis = connectionTimeoutMillis;
            return this;
        }

        public Builder setShutdownTimeoutMillis(long shutdownTimeoutMillis) {
            this.shutdownTimeoutMillis = shutdownTimeoutMillis;
            return this;
        }

        Builder setPublisherFactory(Function<AmqpPublisherConfig, AmqpLogPublisher> publisherFactory) {
            this.publisherFactory = publisherFactory;
            return this;
        }

        @Override
        public MdLoggingAmqpAppender build() {
            // Returning null on invalid configuration is the log4j plugin convention: the
            // appender is skipped with a status error instead of failing configuration entirely.
            if (getName() == null || getName().isBlank()) {
                StatusLogger.getLogger().error("MdLoggingAmqp appender requires a name");
                return null;
            }
            if (application == null || application.isBlank()) {
                StatusLogger.getLogger().error(
                        "MdLoggingAmqp appender '{}' requires an application attribute", getName());
                return null;
            }
            String resolvedPodName = (podName == null || podName.isBlank())
                    ? defaultPodName(System.getenv())
                    : podName;
            AmqpPublisherConfig config = AmqpPublisherConfig.builder()
                    .host(host)
                    .port(port)
                    .virtualHost(virtualHost)
                    .username(username)
                    .password(password)
                    .exchange(exchange)
                    .routingKey((routingKey == null || routingKey.isBlank()) ? application : routingKey)
                    .declareExchange(declareExchange)
                    .connectionTimeoutMillis(connectionTimeoutMillis)
                    .connectionName("md-logging-java/" + application)
                    .build();
            AsyncDispatcher dispatcher =
                    new AsyncDispatcher(publisherFactory.apply(config), queueCapacity, 200, 30_000);
            return new MdLoggingAmqpAppender(getName(), getFilter(), isIgnoreExceptions(),
                    getPropertyArray(), new LogEnvelopeEncoder(application, resolvedPodName, maxEnvelopeBytes),
                    new LogEventConverter(), dispatcher, shutdownTimeoutMillis);
        }

        /** Kubernetes sets HOSTNAME to the pod name; outside a cluster there is no pod. */
        static String defaultPodName(Map<String, String> env) {
            String hostname = env.get("HOSTNAME");
            return (hostname == null || hostname.isBlank()) ? "unknown" : hostname;
        }
    }
}
