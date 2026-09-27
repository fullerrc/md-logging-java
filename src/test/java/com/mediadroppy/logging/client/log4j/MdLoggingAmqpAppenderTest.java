package com.mediadroppy.logging.client.log4j;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mediadroppy.logging.client.amqp.AmqpPublisherConfig;
import com.mediadroppy.logging.client.support.RecordingPublisher;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.util.SortedArrayStringMap;
import org.apache.logging.log4j.util.StringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class MdLoggingAmqpAppenderTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final RecordingPublisher publisher = new RecordingPublisher();
    private MdLoggingAmqpAppender appender;

    @AfterEach
    void tearDown() {
        if (appender != null && !appender.isStopped()) {
            appender.stop();
        }
    }

    @Test
    void builderRejectsMissingName() {
        assertThat(MdLoggingAmqpAppender.newBuilder().setApplication("md-user").build()).isNull();
    }

    @Test
    void builderRejectsMissingApplication() {
        assertThat(MdLoggingAmqpAppender.newBuilder().setName("md").build()).isNull();
    }

    @Test
    void appendPublishesADecodableEnvelope() throws Exception {
        appender = builderWithPublisher()
                .setApplication("md-user")
                .setPodName("pod-9")
                .build();
        appender.start();

        StringMap contextData = new SortedArrayStringMap();
        contextData.putValue("x_forwarded_for", "203.0.113.7");
        appender.append(Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Api")
                .setLevel(Level.INFO)
                .setThreadName("http-1")
                .setMessage(new SimpleMessage("handled"))
                .setContextData(contextData)
                .setTimeMillis(1_722_600_000_123L)
                .build());

        await().atMost(5, SECONDS).until(() -> publisher.published().size() == 1);
        JsonNode outer = mapper.readTree(publisher.published().get(0));
        assertThat(outer.get("messageType").textValue()).isEqualTo("LOG");
        JsonNode inner = mapper.readTree(Base64.getDecoder().decode(outer.get("messageContent").textValue()));
        assertThat(inner.get("timestamp").longValue()).isEqualTo(1_722_600_000_123L);
        assertThat(inner.get("application").textValue()).isEqualTo("md-user");
        assertThat(inner.get("podName").textValue()).isEqualTo("pod-9");
        assertThat(inner.at("/logEntry/message").textValue()).isEqualTo("handled");
        assertThat(inner.at("/logEntry/mdc/x_forwarded_for").textValue()).isEqualTo("203.0.113.7");
    }

    @Test
    void publisherConfigDefaultsRoutingKeyToTheApplication() {
        AtomicReference<AmqpPublisherConfig> seen = new AtomicReference<>();
        appender = MdLoggingAmqpAppender.newBuilder()
                .setPublisherFactory(config -> {
                    seen.set(config);
                    return publisher;
                })
                .setName("md")
                .setApplication("md-user")
                .setHost("rabbit.example")
                .setPort(5673)
                .build();

        AmqpPublisherConfig config = seen.get();
        assertThat(config.routingKey()).isEqualTo("md-user");
        assertThat(config.connectionName()).isEqualTo("md-logging-java/md-user");
        assertThat(config.host()).isEqualTo("rabbit.example");
        assertThat(config.port()).isEqualTo(5673);
        assertThat(config.exchange()).isEqualTo("logging-events");
        assertThat(config.declareExchange()).isTrue();
    }

    @Test
    void explicitRoutingKeyWinsOverTheApplicationDefault() {
        AtomicReference<AmqpPublisherConfig> seen = new AtomicReference<>();
        appender = MdLoggingAmqpAppender.newBuilder()
                .setPublisherFactory(config -> {
                    seen.set(config);
                    return publisher;
                })
                .setName("md")
                .setApplication("md-user")
                .setRoutingKey("logs.custom")
                .build();

        assertThat(seen.get().routingKey()).isEqualTo("logs.custom");
    }

    @Test
    void podNameDefaultsToTheHostnameEnvironmentVariable() {
        assertThat(MdLoggingAmqpAppender.Builder.defaultPodName(Map.of("HOSTNAME", "md-user-6f7b9-x2v")))
                .isEqualTo("md-user-6f7b9-x2v");
        assertThat(MdLoggingAmqpAppender.Builder.defaultPodName(Map.of())).isEqualTo("unknown");
    }

    @Test
    void encodingFailuresAreSwallowedByDefault() {
        appender = builderWithPublisher().setApplication("md-user").build();
        appender.start();

        LogEvent broken = mock(LogEvent.class);
        when(broken.getMessage()).thenThrow(new RuntimeException("unrenderable message"));

        appender.append(broken); // must not throw
        assertThat(publisher.published()).isEmpty();
    }

    @Test
    void encodingFailuresPropagateWhenIgnoreExceptionsIsFalse() {
        appender = builderWithPublisher()
                .setApplication("md-user")
                .setIgnoreExceptions(false)
                .build();
        appender.start();

        LogEvent broken = mock(LogEvent.class);
        when(broken.getMessage()).thenThrow(new RuntimeException("unrenderable message"));

        assertThatThrownBy(() -> appender.append(broken))
                .isInstanceOf(AppenderLoggingException.class);
    }

    @Test
    void stopClosesThePublisherAndLateEventsAreCountedAsDropped() {
        appender = builderWithPublisher().setApplication("md-user").build();
        appender.start();
        appender.stop();

        assertThat(publisher.isClosed()).isTrue();

        appender.append(Log4jLogEvent.newBuilder()
                .setLoggerName("late")
                .setLevel(Level.INFO)
                .setMessage(new SimpleMessage("after stop"))
                .build());

        assertThat(appender.droppedCount()).isEqualTo(1);
        assertThat(publisher.published()).isEmpty();
    }

    private MdLoggingAmqpAppender.Builder builderWithPublisher() {
        return MdLoggingAmqpAppender.newBuilder()
                .setPublisherFactory(config -> publisher)
                .setName("md");
    }
}
