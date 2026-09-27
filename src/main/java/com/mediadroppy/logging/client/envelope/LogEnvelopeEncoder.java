package com.mediadroppy.logging.client.envelope;

import com.mediadroppy.logging.client.json.JsonWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds the exact message envelope {@code md-logging-agent} consumes.
 *
 * <p>Outer envelope: {@code {"messageType":"LOG","messageContent":"<base64>"}} where the content
 * is the base64 of the inner envelope {@code {"timestamp":millis,"application":...,"podName":...,
 * "logEntry":{...}}}. The base64 uses the strict RFC 4648 alphabet, which is the first (and
 * preferred) alphabet the agent's decoder tries.
 *
 * <p>The timestamp is sent in <em>milliseconds</em>: the agent disambiguates seconds from
 * milliseconds by magnitude (values {@code >= 1e11} are milliseconds), and epoch millis crossed
 * that boundary in 1973.
 */
public final class LogEnvelopeEncoder {

    /**
     * Matches the agent's default {@code max-decoded-bytes}. An inner envelope larger than this is
     * rejected to the dead-letter queue on the consuming side, so the encoder must never emit one.
     */
    public static final int DEFAULT_MAX_ENVELOPE_BYTES = 1_048_576;

    private final String application;
    private final String podName;
    private final int maxEnvelopeBytes;

    public LogEnvelopeEncoder(String application, String podName, int maxEnvelopeBytes) {
        if (application == null || application.isBlank()) {
            throw new IllegalArgumentException("application must not be blank");
        }
        if (maxEnvelopeBytes <= 0) {
            throw new IllegalArgumentException("maxEnvelopeBytes must be positive");
        }
        this.application = application;
        this.podName = (podName == null || podName.isBlank()) ? "unknown" : podName;
        this.maxEnvelopeBytes = maxEnvelopeBytes;
    }

    /** @return the outer envelope as UTF-8 bytes, ready to publish as an AMQP body */
    public byte[] encode(long timestampMillis, Map<String, Object> logEntry) {
        byte[] inner = innerEnvelope(timestampMillis, logEntry == null ? Map.of() : logEntry);
        if (inner.length > maxEnvelopeBytes) {
            // The agent dead-letters oversized envelopes, which silently loses the record. A stub
            // that says the entry was too big preserves at least the fact that something happened.
            inner = innerEnvelope(timestampMillis, oversizeFallback(logEntry, inner.length));
        }
        String content = Base64.getEncoder().encodeToString(inner);
        // The base64 alphabet contains nothing JSON needs to escape, so direct embedding is safe.
        return ("{\"messageType\":\"LOG\",\"messageContent\":\"" + content + "\"}")
                .getBytes(StandardCharsets.UTF_8);
    }

    private byte[] innerEnvelope(long timestampMillis, Map<String, Object> logEntry) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("timestamp", timestampMillis);
        envelope.put("application", application);
        envelope.put("podName", podName);
        envelope.put("logEntry", logEntry);
        return JsonWriter.write(envelope).getBytes(StandardCharsets.UTF_8);
    }

    /** Keeps the small identifying fields and replaces the payload with an explanation. */
    private Map<String, Object> oversizeFallback(Map<String, Object> logEntry, int actualBytes) {
        Map<String, Object> fallback = new LinkedHashMap<>();
        if (logEntry != null) {
            for (String key : new String[] {"level", "logger", "thread"}) {
                Object value = logEntry.get(key);
                if (value instanceof String) {
                    fallback.put(key, value);
                }
            }
        }
        fallback.put("message", "Log entry of " + actualBytes + " bytes exceeded the "
                + maxEnvelopeBytes + "-byte envelope limit and was replaced with this stub");
        fallback.put("truncated", true);
        return fallback;
    }
}
