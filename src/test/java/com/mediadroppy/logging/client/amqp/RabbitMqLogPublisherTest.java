package com.mediadroppy.logging.client.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RabbitMqLogPublisherTest {

    private static final byte[] BODY = "{\"messageType\":\"LOG\"}".getBytes(StandardCharsets.UTF_8);

    @Mock
    private ConnectionFactory factory;

    @Mock
    private Connection connection;

    @Mock
    private Channel channel;

    private final AmqpPublisherConfig config = AmqpPublisherConfig.builder()
            .routingKey("md-user")
            .connectionName("md-logging-java/md-user")
            .build();

    private RabbitMqLogPublisher publisher() {
        return new RabbitMqLogPublisher(config, factory);
    }

    @Test
    void firstPublishConnectsDeclaresTheExchangeAndPublishesPersistentJson() throws Exception {
        when(factory.newConnection("md-logging-java/md-user")).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);

        publisher().publish(BODY);

        // Durable topic, not auto-delete: byte-identical to the agent's own declaration, so
        // whichever side declares first the other's declaration is a no-op.
        verify(channel).exchangeDeclare("logging-events", "topic", true, false, null);
        ArgumentCaptor<AMQP.BasicProperties> properties = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
        verify(channel).basicPublish(eq("logging-events"), eq("md-user"), properties.capture(), eq(BODY));
        assertThat(properties.getValue().getContentType()).isEqualTo("application/json");
        assertThat(properties.getValue().getDeliveryMode()).isEqualTo(2);
    }

    @Test
    void reusesTheOpenChannelAcrossPublishes() throws Exception {
        when(factory.newConnection(anyString())).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.isOpen()).thenReturn(true);

        RabbitMqLogPublisher publisher = publisher();
        publisher.publish(BODY);
        publisher.publish(BODY);

        verify(factory, times(1)).newConnection(anyString());
        verify(channel, times(2)).basicPublish(anyString(), anyString(), any(), eq(BODY));
    }

    @Test
    void reconnectsWhenTheChannelHasClosed() throws Exception {
        when(factory.newConnection(anyString())).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.isOpen()).thenReturn(false);

        RabbitMqLogPublisher publisher = publisher();
        publisher.publish(BODY);
        publisher.publish(BODY);

        verify(factory, times(2)).newConnection(anyString());
        verify(connection).abort(1_000);
    }

    @Test
    void skipsExchangeDeclarationWhenDisabled() throws Exception {
        AmqpPublisherConfig noDeclare = AmqpPublisherConfig.builder()
                .routingKey("md-user")
                .declareExchange(false)
                .build();
        when(factory.newConnection(anyString())).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);

        new RabbitMqLogPublisher(noDeclare, factory).publish(BODY);

        verify(channel, never()).exchangeDeclare(anyString(), anyString(), anyBoolean(), anyBoolean(), any());
        verify(channel).basicPublish(eq("logging-events"), eq("md-user"), any(), eq(BODY));
    }

    @Test
    void publishFailureDiscardsTheConnectionAndRethrows() throws Exception {
        when(factory.newConnection(anyString())).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        doThrow(new IOException("broker went away"))
                .when(channel).basicPublish(anyString(), anyString(), any(), any(byte[].class));

        RabbitMqLogPublisher publisher = publisher();

        assertThatThrownBy(() -> publisher.publish(BODY)).isInstanceOf(IOException.class);
        verify(connection).abort(1_000);
    }

    @Test
    void connectFailureAbandonsTheHalfOpenConnection() throws Exception {
        when(factory.newConnection(anyString())).thenReturn(connection);
        when(connection.createChannel()).thenThrow(new IOException("no channels"));

        RabbitMqLogPublisher publisher = publisher();

        assertThatThrownBy(() -> publisher.publish(BODY)).isInstanceOf(IOException.class);
        verify(connection).abort(1_000);
    }

    @Test
    void closeAbortsTheConnection() throws Exception {
        when(factory.newConnection(anyString())).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);

        RabbitMqLogPublisher publisher = publisher();
        publisher.publish(BODY);
        publisher.close();

        verify(connection).abort(1_000);
    }

    @Test
    void defaultFactoryCarriesConnectionSettingsAndDisablesAutoRecovery() {
        AmqpPublisherConfig full = AmqpPublisherConfig.builder()
                .host("rabbit.example")
                .port(5673)
                .virtualHost("/logs")
                .username("svc")
                .password("pw")
                .routingKey("rk")
                .connectionTimeoutMillis(1_234)
                .build();

        ConnectionFactory created = RabbitMqLogPublisher.defaultFactory(full);

        assertThat(created.getHost()).isEqualTo("rabbit.example");
        assertThat(created.getPort()).isEqualTo(5673);
        assertThat(created.getVirtualHost()).isEqualTo("/logs");
        assertThat(created.getUsername()).isEqualTo("svc");
        assertThat(created.getPassword()).isEqualTo("pw");
        assertThat(created.getConnectionTimeout()).isEqualTo(1_234);
        assertThat(created.isAutomaticRecoveryEnabled()).isFalse();
    }
}
