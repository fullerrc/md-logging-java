# md-logging-java — Design

Implements `docs/inception.md`: a Log4j 2–compatible library that formats log events into the
message envelope `md-logging-agent` consumes and publishes them to an AMQP exchange the consuming
service has access to.

## 0. Pinned decisions

| Item | Value | Rationale |
|---|---|---|
| groupId / artifactId | `com.mediadroppy` / `md-logging-java` | matches the family |
| Base package | `com.mediadroppy.logging.client` | `com.mediadroppy.logging` is the agent's |
| Version | `1.0-SNAPSHOT` | matches siblings; CI stamps real versions |
| Java release | **21** | what the consuming services build and run on |
| Log4j | 2.25.x (`log4j-core`) | the "interface compatible with Log4J" — consumers instantiate loggers through the standard Log4j API and only add an appender |
| AMQP client | `com.rabbitmq:amqp-client` 5.x | no Spring dependency; the library must embed into any codebase |
| JSON | hand-rolled `JsonWriter` | shipping Jackson would pin consumers to a Jackson major version mid–Jackson-2→3 migration; the emitted structures are entirely library-built, and the test suite verifies output against a real parser (Jackson 2, test scope only) |

**Why an appender, not a logger facade.** Inception asks for "an interface compatible with Log4J,
such that a logger can be instantiated within a consuming codebase". A custom `@Plugin` appender is
the Log4j-native seam: consumers keep `LogManager.getLogger(...)`, their existing configuration,
levels and filters, and add one element to reach the pipeline. A bespoke logger wrapper would be a
*second* logging API to migrate to; an appender is configuration.

## 1. Envelope compatibility

The contract is `md-logging-agent`'s `EnvelopeDecoder`, mirrored gate by gate:

| Agent decoder gate | Producer guarantee |
|---|---|
| Raw size ≤ `max-encoded-bytes` | inner envelope capped at `maxEnvelopeBytes` (default 1 MiB, the agent's `max-decoded-bytes`) |
| Outer JSON object | built as `{"messageType":"LOG","messageContent":"<base64>"}` |
| `messageType` == `LOG` (string) | constant |
| `messageContent` strict-base64 | `Base64.getEncoder()` — RFC 4648 basic alphabet, the first the agent tries |
| Inner JSON object | `{timestamp, application, podName, logEntry}` |
| `timestamp` numeric | epoch **milliseconds** — `>= 1e11`, so magnitude disambiguation always reads it as millis |
| `application` / `podName` scalar strings | constructor-validated; `podName` falls back `HOSTNAME` → `unknown` |
| `logEntry` object | always an object, `{}` when there is nothing to say |

`AgentContractTest` walks an emitted envelope through those gates in order, so producer-side drift
fails in this repo, not in the agent's dead-letter queue.

An entry that would exceed `maxEnvelopeBytes` (a pathological message or MDC) is re-encoded as a
stub — `level`/`logger`/`thread` preserved, `truncated: true`, message explaining the size —
because the alternative is the agent rejecting the whole envelope to its DLQ, silently losing the
record.

The `logEntry` layout is `{level, logger, thread, message, marker?, mdc?, throwable?}`. MDC passes
through under `mdc`; the agent's `x_forwarded_for` extractor is recursive over `logEntry`, so a
service that puts the header into `ThreadContext` gets its client IPs indexed with no further work.

## 2. Class layout

All paths relative to `src/main/java/com/mediadroppy/logging/client/`.

| File | Responsibility |
|---|---|
| `json/JsonWriter.java` | Minimal JSON emitter: maps, iterables, strings (fully escaped), numbers (non-finite → `null`), booleans; anything else stringified |
| `envelope/LogEnvelopeEncoder.java` | Inner + outer envelope bytes; oversize stub substitution |
| `amqp/AmqpLogPublisher.java` | One-method publish interface; the dispatcher's test seam |
| `amqp/AmqpPublisherConfig.java` | Validated record; `toString` masks the password |
| `amqp/RabbitMqLogPublisher.java` | Lazy connect, declares the exchange exactly as the agent does (durable topic), persistent `application/json` publishes, discards broken channels so the next call reconnects |
| `dispatch/AsyncDispatcher.java` | Bounded queue + single daemon worker; retry with exponential backoff; drop-and-count on overflow; graceful drain on stop |
| `log4j/LogEventConverter.java` | `LogEvent` → `logEntry` map, materialised eagerly (log4j may reuse the event) |
| `log4j/MdLoggingAmqpAppender.java` | The `MdLoggingAmqp` plugin: builder, validation, lifecycle |
| `log4j/MdLogging.java` | Programmatic install/uninstall on the root logger |

## 3. Failure modes

A logging library's cardinal rule: never make the application worse. Every failure path resolves
to "drop, count, status-log" — never block, never throw into application code, never grow without
bound.

| Condition | Handling |
|---|---|
| Broker down at startup | Connection is lazy; events buffer, worker retries with backoff (200 ms → 30 s cap) |
| Broker down in flight | Publish failure discards the channel; event is retained and retried; queue absorbs up to `queueCapacity` |
| Queue full | New events dropped and counted; warning on first drop and every 1000th |
| Event cannot be encoded | `error()` to the status logger; rethrow only when `ignoreExceptions="false"` (standard Log4j semantics) |
| Entry exceeds size limit | Stub substitution (§1) |
| Shutdown | Worker drains within `shutdownTimeoutMillis`, then remaining events are counted dropped and the connection aborted |
| AMQP client logs routed back into log4j | Events raised on the dispatch thread are ignored by `append`, so the pipeline cannot consume its own failure reports |

Internal reporting uses Log4j's `StatusLogger` exclusively — logging through a Log4j `Logger` from
inside an appender would recurse into the appender.

The connection disables the AMQP client's automatic recovery: the dispatcher already owns
reconnection, and two recovery loops fighting over one connection is how duplicate channels and
stuck publishes happen.

## 4. Test strategy

**Unit** (all always-on): `JsonWriter` (escaping verified through a real parser), `LogEnvelopeEncoder`
(shape, strict base64, oversize stub), `LogEventConverter` (fields, MDC, marker, cause chains),
`AsyncDispatcher` (ordering, retry, overflow drops, prompt shutdown with a dead broker, dispatch-thread
identity), `RabbitMqLogPublisher` (mocked AMQP client: declaration args, persistent JSON properties,
reconnect-after-failure, abort on close), appender builder validation and end-to-end
append→envelope, `MdLogging` install/uninstall on a private `LoggerContext`.

**`AgentContractTest`**: the decoder-gate walk from §1.

**`XmlConfigurationTest`**: loads the appender from an XML string *without* a `packages` attribute,
proving the compile-time plugin descriptor (`Log4j2Plugins.dat`, generated by log4j's annotation
processor under `-proc:full`) works — this is exactly how consumers discover the plugin. Also
exercises append-with-unreachable-broker and bounded shutdown.

**Integration** (`@Tag("testcontainers")`, gated on Docker detection): real `rabbitmq:4-management-alpine`;
the appender publishes through its real publisher, a consumer bound like the agent (durable topic
`logging-events`, routing key `#`) receives; asserts routing key, content type, delivery mode, and
full envelope decode. Skips silently without Docker (colima users: see README for the two env vars);
CI always runs it.

## 5. CI

`maven-test.yml` (reusable, JDK 21 temurin, `mvn verify`) is called by `pr-build.yml` (PRs to
`master`; also attaches the jar to the run) and `main-build.yml` (pushes to `master`; computes the
next `X.Y.Z` from git tags exactly like the sibling services, packages the versioned jar via
`versions:set`, uploads it, and pushes the tag only after the build succeeds, so a tag always marks
a good artifact). There is no Maven registry in the family's infrastructure yet; the tagged jar on
the workflow run is the publishable artifact.
