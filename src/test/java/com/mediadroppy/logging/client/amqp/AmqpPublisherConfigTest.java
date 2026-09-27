package com.mediadroppy.logging.client.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

class AmqpPublisherConfigTest {

    @Test
    void defaultsMatchTheAgentTopology() {
        AmqpPublisherConfig config = AmqpPublisherConfig.builder().build();
        assertThat(config.host()).isEqualTo("localhost");
        assertThat(config.port()).isEqualTo(5672);
        assertThat(config.virtualHost()).isEqualTo("/");
        assertThat(config.exchange()).isEqualTo("logging-events");
        assertThat(config.declareExchange()).isTrue();
    }

    @Test
    void rejectsBlankHost() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> AmqpPublisherConfig.builder().host(" ").build());
    }

    @Test
    void rejectsOutOfRangePort() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> AmqpPublisherConfig.builder().port(0).build());
        assertThatIllegalArgumentException()
                .isThrownBy(() -> AmqpPublisherConfig.builder().port(65536).build());
    }

    @Test
    void rejectsBlankExchangeAndNullRoutingKey() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> AmqpPublisherConfig.builder().exchange("").build());
        assertThatIllegalArgumentException()
                .isThrownBy(() -> AmqpPublisherConfig.builder().routingKey(null).build());
    }

    @Test
    void toStringMasksThePassword() {
        AmqpPublisherConfig config = AmqpPublisherConfig.builder().password("s3cret!").build();
        assertThat(config.toString()).doesNotContain("s3cret!").contains("password=*****");
    }
}
