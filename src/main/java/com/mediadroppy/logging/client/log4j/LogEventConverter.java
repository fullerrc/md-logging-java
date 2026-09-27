package com.mediadroppy.logging.client.log4j;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.ReadOnlyStringMap;

/**
 * Renders a Log4j {@link LogEvent} into the {@code logEntry} structure of the inner envelope.
 *
 * <p>Everything is materialised eagerly: log4j may mutate or reuse the event object after
 * {@code append} returns, so nothing from the event can be referenced by the queued payload.
 *
 * <p>The thread-context (MDC) map is passed through under {@code mdc}. That is where the agent's
 * {@code x_forwarded_for} extraction finds the client IP — its key matching is recursive over the
 * whole {@code logEntry} — so a web service only has to put the header value into
 * {@code ThreadContext} for it to end up indexed.
 */
final class LogEventConverter {

    Map<String, Object> toLogEntry(LogEvent event) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("level", event.getLevel() == null ? null : event.getLevel().name());
        entry.put("logger", event.getLoggerName());
        entry.put("thread", event.getThreadName());
        Message message = event.getMessage();
        entry.put("message", message == null ? null : message.getFormattedMessage());
        if (event.getMarker() != null) {
            entry.put("marker", event.getMarker().getName());
        }
        ReadOnlyStringMap contextData = event.getContextData();
        if (contextData != null && !contextData.isEmpty()) {
            entry.put("mdc", new LinkedHashMap<String, String>(contextData.toMap()));
        }
        Throwable thrown = event.getThrown();
        if (thrown != null) {
            entry.put("throwable", throwableEntry(thrown));
        }
        return entry;
    }

    private Map<String, Object> throwableEntry(Throwable thrown) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("class", thrown.getClass().getName());
        entry.put("message", thrown.getMessage());
        StringWriter trace = new StringWriter(512);
        thrown.printStackTrace(new PrintWriter(trace)); // includes causes and suppressed
        entry.put("stackTrace", trace.toString());
        return entry;
    }
}
