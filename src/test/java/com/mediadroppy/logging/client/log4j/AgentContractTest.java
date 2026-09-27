package com.mediadroppy.logging.client.log4j;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mediadroppy.logging.client.envelope.LogEnvelopeEncoder;
import java.util.Base64;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.SortedArrayStringMap;
import org.apache.logging.log4j.util.StringMap;
import org.junit.jupiter.api.Test;

/**
 * Walks an emitted envelope through every gate of {@code md-logging-agent}'s
 * {@code EnvelopeDecoder}, in order, so that producer-side drift from the consumer contract fails
 * here rather than in a dead-letter queue.
 */
class AgentContractTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void emittedEnvelopePassesEveryDecoderGate() throws Exception {
        StringMap contextData = new SortedArrayStringMap();
        contextData.putValue("x_forwarded_for", "203.0.113.7");
        long timeMillis = System.currentTimeMillis();
        Log4jLogEvent event = Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Gateway")
                .setLevel(Level.ERROR)
                .setThreadName("http-9")
                .setMessage(new ParameterizedMessage("client {} rejected", "203.0.113.7"))
                .setContextData(contextData)
                .setThrown(new IllegalStateException("session store unavailable"))
                .setTimeMillis(timeMillis)
                .build();

        byte[] body = new LogEnvelopeEncoder("md-user", "md-user-6f7b9-x2v",
                LogEnvelopeEncoder.DEFAULT_MAX_ENVELOPE_BYTES)
                .encode(event.getTimeMillis(), new LogEventConverter().toLogEntry(event));

        // Gate 1: the body is non-empty and within the agent's raw-size bound.
        assertThat(body).isNotEmpty();
        assertThat(body.length).isLessThanOrEqualTo(LogEnvelopeEncoder.DEFAULT_MAX_ENVELOPE_BYTES * 4 / 3 + 4);

        // Gate 2: the outer envelope parses as a JSON object.
        JsonNode outer = mapper.readTree(body);
        assertThat(outer.isObject()).isTrue();

        // Gate 3: messageType is the string LOG — anything else is silently dropped.
        JsonNode messageType = outer.get("messageType");
        assertThat(messageType).isNotNull();
        assertThat(messageType.isTextual()).isTrue();
        assertThat(messageType.textValue()).isEqualTo("LOG");

        // Gate 4: messageContent is a string decodable by the strict RFC 4648 decoder, which the
        // agent tries before falling back to the URL-safe alphabet.
        JsonNode messageContent = outer.get("messageContent");
        assertThat(messageContent).isNotNull();
        assertThat(messageContent.isTextual()).isTrue();
        byte[] decoded = Base64.getDecoder().decode(messageContent.textValue());

        // Gate 5: the inner envelope parses as a JSON object.
        JsonNode inner = mapper.readTree(decoded);
        assertThat(inner.isObject()).isTrue();

        // Gate 6: the timestamp is an integral JSON number, and its magnitude puts it on the
        // milliseconds side of the agent's seconds/milliseconds disambiguation (>= 1e11).
        JsonNode timestamp = inner.get("timestamp");
        assertThat(timestamp.isIntegralNumber()).isTrue();
        assertThat(timestamp.canConvertToLong()).isTrue();
        assertThat(timestamp.longValue()).isEqualTo(timeMillis).isGreaterThanOrEqualTo(100_000_000_000L);

        // Gate 7: application and podName are scalar strings.
        assertThat(inner.get("application").isTextual()).isTrue();
        assertThat(inner.get("application").textValue()).isEqualTo("md-user");
        assertThat(inner.get("podName").isTextual()).isTrue();

        // Gate 8: logEntry is a JSON object carrying the event.
        JsonNode logEntry = inner.get("logEntry");
        assertThat(logEntry.isObject()).isTrue();
        assertThat(logEntry.get("message").textValue()).isEqualTo("client 203.0.113.7 rejected");

        // And the field the pipeline exists to index: the agent's recursive x_forwarded_for
        // extractor finds MDC values nested anywhere under logEntry.
        assertThat(inner.at("/logEntry/mdc/x_forwarded_for").textValue()).isEqualTo("203.0.113.7");
    }
}
