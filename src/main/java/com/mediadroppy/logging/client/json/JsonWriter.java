package com.mediadroppy.logging.client.json;

import java.util.Map;

/**
 * Minimal JSON emitter for the envelope and log-entry structures this library builds itself.
 *
 * <p>Hand-rolled on purpose: a logging client gets embedded into every service, and shipping a
 * JSON library from here would pin consumers to a Jackson major version they may not use (the
 * MediaDroppy services are mid-migration from Jackson 2 to Jackson 3, which have different
 * coordinates <em>and</em> different packages). The structures serialized here are built by this
 * library — maps, lists, strings, numbers, booleans — so a small, fully tested emitter covers
 * them; correctness against a real parser is asserted in the test suite.
 */
public final class JsonWriter {

    private JsonWriter() {
    }

    public static String write(Object value) {
        StringBuilder out = new StringBuilder(64);
        append(out, value);
        return out.toString();
    }

    /**
     * Appends {@code value} as JSON. Maps become objects (keys stringified), {@link Iterable}s
     * become arrays, and any type outside the JSON model falls back to its {@code toString} as a
     * JSON string — a logging pipeline must degrade to "stringify it" rather than throw.
     */
    public static void append(StringBuilder out, Object value) {
        switch (value) {
            case null -> out.append("null");
            case String s -> appendString(out, s);
            case Boolean b -> out.append(b.booleanValue());
            case Double d -> appendFinite(out, d);
            case Float f -> appendFinite(out, f.doubleValue());
            case Number n -> out.append(n);
            case Map<?, ?> map -> appendObject(out, map);
            case Iterable<?> iterable -> appendArray(out, iterable);
            default -> appendString(out, String.valueOf(value));
        }
    }

    private static void appendObject(StringBuilder out, Map<?, ?> map) {
        out.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) {
                out.append(',');
            }
            first = false;
            appendString(out, String.valueOf(entry.getKey()));
            out.append(':');
            append(out, entry.getValue());
        }
        out.append('}');
    }

    private static void appendArray(StringBuilder out, Iterable<?> iterable) {
        out.append('[');
        boolean first = true;
        for (Object element : iterable) {
            if (!first) {
                out.append(',');
            }
            first = false;
            append(out, element);
        }
        out.append(']');
    }

    /** JSON has no representation for NaN or infinities; {@code null} is the lossless-enough stand-in. */
    private static void appendFinite(StringBuilder out, double value) {
        if (Double.isFinite(value)) {
            out.append(value);
        } else {
            out.append("null");
        }
    }

    private static void appendString(StringBuilder out, String value) {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }
}
