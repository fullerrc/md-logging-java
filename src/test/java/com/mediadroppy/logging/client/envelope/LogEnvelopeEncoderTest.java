package com.mediadroppy.logging.client.envelope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LogEnvelopeEncoderTest {

    private static final long TIMESTAMP = 1_722_600_000_123L;

    private final ObjectMapper mapper = new ObjectMapper();
    private final LogEnvelopeEncoder encoder =
            new LogEnvelopeEncoder("md-user", "md-user-6f7b9-x2v", LogEnvelopeEncoder.DEFAULT_MAX_ENVELOPE_BYTES);

    @Test
    void outerEnvelopeWrapsBase64InnerEnvelope() throws Exception {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("level", "INFO");
        entry.put("message", "hello \"world\"\nsecond line");

        JsonNode outer = mapper.readTree(encoder.encode(TIMESTAMP, entry));

        assertThat(outer.get("messageType").textValue()).isEqualTo("LOG");
        JsonNode inner = decodeInner(outer);
        assertThat(inner.get("timestamp").longValue()).isEqualTo(TIMESTAMP);
        assertThat(inner.get("application").textValue()).isEqualTo("md-user");
        assertThat(inner.get("podName").textValue()).isEqualTo("md-user-6f7b9-x2v");
        assertThat(inner.get("logEntry").get("level").textValue()).isEqualTo("INFO");
        assertThat(inner.get("logEntry").get("message").textValue())
                .isEqualTo("hello \"world\"\nsecond line");
    }

    @Test
    void base64UsesTheStrictAlphabetTheAgentTriesFirst() throws Exception {
        JsonNode outer = mapper.readTree(encoder.encode(TIMESTAMP, Map.of("k", "v")));
        // RFC 4648 basic alphabet only — never the URL-safe or MIME variants.
        assertThat(outer.get("messageContent").textValue()).matches("^[A-Za-z0-9+/]+={0,2}$");
    }

    @Test
    void nullLogEntryBecomesEmptyObject() throws Exception {
        JsonNode inner = decodeInner(mapper.readTree(encoder.encode(TIMESTAMP, null)));
        assertThat(inner.get("logEntry").isObject()).isTrue();
        assertThat(inner.get("logEntry").isEmpty()).isTrue();
    }

    @Test
    void blankPodNameFallsBackToUnknown() throws Exception {
        LogEnvelopeEncoder blankPod = new LogEnvelopeEncoder("app", " ", 4096);
        JsonNode inner = decodeInner(mapper.readTree(blankPod.encode(TIMESTAMP, Map.of())));
        assertThat(inner.get("podName").textValue()).isEqualTo("unknown");
    }

    @Test
    void oversizeLogEntryIsReplacedWithAStubInsteadOfBeingDeadLettered() throws Exception {
        LogEnvelopeEncoder small = new LogEnvelopeEncoder("md-user", "pod-1", 512);
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("level", "ERROR");
        entry.put("logger", "com.example.Big");
        entry.put("thread", "main");
        entry.put("message", "x".repeat(10_000));

        JsonNode outer = mapper.readTree(small.encode(TIMESTAMP, entry));
        byte[] innerBytes = Base64.getDecoder().decode(outer.get("messageContent").textValue());

        // The whole point: what actually goes on the wire fits the consumer's rejection bound.
        assertThat(innerBytes.length).isLessThanOrEqualTo(512);
        JsonNode logEntry = mapper.readTree(innerBytes).get("logEntry");
        assertThat(logEntry.get("truncated").booleanValue()).isTrue();
        assertThat(logEntry.get("level").textValue()).isEqualTo("ERROR");
        assertThat(logEntry.get("logger").textValue()).isEqualTo("com.example.Big");
        assertThat(logEntry.get("thread").textValue()).isEqualTo("main");
        assertThat(logEntry.get("message").textValue()).contains("exceeded the 512-byte envelope limit");
    }

    @Test
    void withinLimitEntriesAreNotTruncated() throws Exception {
        LogEnvelopeEncoder small = new LogEnvelopeEncoder("md-user", "pod-1", 4096);
        JsonNode inner = decodeInner(mapper.readTree(small.encode(TIMESTAMP, Map.of("message", "short"))));
        assertThat(inner.get("logEntry").has("truncated")).isFalse();
    }

    @Test
    void rejectsBlankApplication() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new LogEnvelopeEncoder(" ", "pod", 1024));
    }

    @Test
    void rejectsNonPositiveLimit() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new LogEnvelopeEncoder("app", "pod", 0));
    }

    private JsonNode decodeInner(JsonNode outer) throws Exception {
        byte[] inner = Base64.getDecoder().decode(outer.get("messageContent").textValue());
        return mapper.readTree(new String(inner, StandardCharsets.UTF_8));
    }
}
