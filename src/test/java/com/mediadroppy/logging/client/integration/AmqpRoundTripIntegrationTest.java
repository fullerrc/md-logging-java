package com.mediadroppy.logging.client.integration;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mediadroppy.logging.client.log4j.MdLoggingAmqpAppender;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.util.SortedArrayStringMap;
import org.apache.logging.log4j.util.StringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end against a real broker: the appender publishes through its real RabbitMQ publisher,
 * and a consumer bound the way the agent binds (durable topic exchange {@code logging-events},
 * routing key {@code #}) receives and decodes the envelope.
 *
 * <p>Gated on Docker availability so a developer without Docker still gets a green build; CI runs
 * it.
 */
@Tag("testcontainers")
class AmqpRoundTripIntegrationTest {

    private static RabbitMQContainer rabbit;

    @BeforeAll
    static void startBroker() {
        assumeTrue(dockerAvailable(), "Docker is not available; skipping container-backed test");
        rabbit = new RabbitMQContainer(DockerImageName.parse("rabbitmq:4-management-alpine"));
        rabbit.start();
    }

    @AfterAll
    static void stopBroker() {
        if (rabbit != null) {
            rabbit.stop();
        }
    }

    private static boolean dockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable unavailable) {
            return false;
        }
    }

    @Test
    void envelopeRoundTripsThroughARealBrokerToAnAgentStyleConsumer() throws Exception {
        MdLoggingAmqpAppender appender = MdLoggingAmqpAppender.newBuilder()
                .setName("it")
                .setApplication("it-app")
                .setPodName("it-pod")
                .setHost(rabbit.getHost())
                .setPort(rabbit.getAmqpPort())
                .setUsername(rabbit.getAdminUsername())
                .setPassword(rabbit.getAdminPassword())
                .build();
        appender.start();

        ConnectionFactory consumerFactory = new ConnectionFactory();
        consumerFactory.setHost(rabbit.getHost());
        consumerFactory.setPort(rabbit.getAmqpPort());
        consumerFactory.setUsername(rabbit.getAdminUsername());
        consumerFactory.setPassword(rabbit.getAdminPassword());

        try (Connection connection = consumerFactory.newConnection("it-consumer")) {
            Channel channel = connection.createChannel();
            // The agent's declaration; the appender must co-declare compatibly on connect.
            channel.exchangeDeclare("logging-events", "topic", true, false, null);
            String queue = channel.queueDeclare().getQueue();
            channel.queueBind(queue, "logging-events", "#");

            StringMap contextData = new SortedArrayStringMap();
            contextData.putValue("x_forwarded_for", "203.0.113.7");
            appender.append(Log4jLogEvent.newBuilder()
                    .setLoggerName("com.example.It")
                    .setLevel(Level.INFO)
                    .setThreadName("it-thread")
                    .setMessage(new SimpleMessage("round trip"))
                    .setContextData(contextData)
                    .setTimeMillis(1_722_600_000_123L)
                    .build());

            AtomicReference<GetResponse> received = new AtomicReference<>();
            await().atMost(15, SECONDS).until(() -> {
                GetResponse response = channel.basicGet(queue, true);
                if (response != null) {
                    received.set(response);
                    return true;
                }
                return false;
            });

            GetResponse response = received.get();
            assertThat(response.getEnvelope().getRoutingKey()).isEqualTo("it-app");
            assertThat(response.getProps().getContentType()).isEqualTo("application/json");
            assertThat(response.getProps().getDeliveryMode()).isEqualTo(2);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode outer = mapper.readTree(response.getBody());
            assertThat(outer.get("messageType").textValue()).isEqualTo("LOG");
            JsonNode inner = mapper.readTree(
                    Base64.getDecoder().decode(outer.get("messageContent").textValue()));
            assertThat(inner.get("timestamp").longValue()).isEqualTo(1_722_600_000_123L);
            assertThat(inner.get("application").textValue()).isEqualTo("it-app");
            assertThat(inner.get("podName").textValue()).isEqualTo("it-pod");
            assertThat(inner.at("/logEntry/message").textValue()).isEqualTo("round trip");
            assertThat(inner.at("/logEntry/mdc/x_forwarded_for").textValue()).isEqualTo("203.0.113.7");
        } finally {
            appender.stop();
        }
    }
}
