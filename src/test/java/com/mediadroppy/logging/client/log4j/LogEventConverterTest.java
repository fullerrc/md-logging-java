package com.mediadroppy.logging.client.log4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.util.SortedArrayStringMap;
import org.apache.logging.log4j.util.StringMap;
import org.junit.jupiter.api.Test;

class LogEventConverterTest {

    private final LogEventConverter converter = new LogEventConverter();

    @Test
    void carriesTheCoreEventFields() {
        Log4jLogEvent event = Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Api")
                .setLevel(Level.WARN)
                .setThreadName("http-1")
                .setMessage(new ParameterizedMessage("user {} rejected", "bob"))
                .setTimeMillis(123L)
                .build();

        Map<String, Object> entry = converter.toLogEntry(event);

        assertThat(entry.get("level")).isEqualTo("WARN");
        assertThat(entry.get("logger")).isEqualTo("com.example.Api");
        assertThat(entry.get("thread")).isEqualTo("http-1");
        assertThat(entry.get("message")).isEqualTo("user bob rejected");
    }

    @Test
    void omitsOptionalFieldsWhenAbsent() {
        Log4jLogEvent event = Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Quiet")
                .setLevel(Level.INFO)
                .setMessage(new SimpleMessage("plain"))
                .build();

        Map<String, Object> entry = converter.toLogEntry(event);

        assertThat(entry).containsOnlyKeys("level", "logger", "thread", "message");
    }

    @Test
    void passesTheContextMapThroughAsMdc() {
        StringMap contextData = new SortedArrayStringMap();
        contextData.putValue("x_forwarded_for", "203.0.113.7, 10.0.0.1");
        contextData.putValue("requestId", "r-42");
        Log4jLogEvent event = Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Api")
                .setLevel(Level.INFO)
                .setMessage(new SimpleMessage("request handled"))
                .setContextData(contextData)
                .build();

        Map<String, Object> entry = converter.toLogEntry(event);

        assertThat(entry.get("mdc")).isEqualTo(Map.of(
                "x_forwarded_for", "203.0.113.7, 10.0.0.1",
                "requestId", "r-42"));
    }

    @Test
    void carriesTheMarkerName() {
        Log4jLogEvent event = Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Audit")
                .setLevel(Level.INFO)
                .setMessage(new SimpleMessage("audited"))
                .setMarker(MarkerManager.getMarker("SECURITY"))
                .build();

        assertThat(converter.toLogEntry(event).get("marker")).isEqualTo("SECURITY");
    }

    @Test
    @SuppressWarnings("unchecked")
    void rendersTheFullCauseChainOfAThrowable() {
        IllegalStateException thrown =
                new IllegalStateException("boom", new IllegalArgumentException("root cause"));
        Log4jLogEvent event = Log4jLogEvent.newBuilder()
                .setLoggerName("com.example.Failing")
                .setLevel(Level.ERROR)
                .setMessage(new SimpleMessage("it broke"))
                .setThrown(thrown)
                .build();

        Map<String, Object> throwable = (Map<String, Object>) converter.toLogEntry(event).get("throwable");

        assertThat(throwable.get("class")).isEqualTo("java.lang.IllegalStateException");
        assertThat(throwable.get("message")).isEqualTo("boom");
        assertThat((String) throwable.get("stackTrace"))
                .contains("java.lang.IllegalStateException: boom")
                .contains("Caused by: java.lang.IllegalArgumentException: root cause");
    }
}
