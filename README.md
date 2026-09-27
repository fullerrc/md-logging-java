# md-logging-java

A Log4j 2 appender that wraps log events in the message envelope expected by
[`md-logging-agent`](../md-logging-agent) and publishes them to an AMQP topic exchange. Add the
appender to your Log4j configuration, instantiate loggers exactly as you already do, and every
event flows to the centralized logging pipeline.

```
your code → LogManager.getLogger(...) → MdLoggingAmqp appender → RabbitMQ (logging-events) → md-logging-agent → MongoDB
```

## Message contract

Each event is published as the two-layer envelope the agent decodes:

```json
{ "messageType": "LOG", "messageContent": "<base64 of the inner envelope>" }
```

The inner envelope:

```json
{
  "timestamp": 1722600000123,
  "application": "md-user",
  "podName": "md-user-6f7b9-x2v",
  "logEntry": {
    "level": "ERROR",
    "logger": "com.example.Api",
    "thread": "http-1",
    "message": "rendered message",
    "marker": "SECURITY",
    "mdc": { "x_forwarded_for": "203.0.113.7" },
    "throwable": { "class": "...", "message": "...", "stackTrace": "..." }
  }
}
```

`timestamp` is epoch **milliseconds** (the agent disambiguates by magnitude). `marker`, `mdc`, and
`throwable` appear only when present. Put request metadata — notably `x_forwarded_for` — into the
Log4j `ThreadContext` (MDC); the agent finds it anywhere under `logEntry` and indexes the client IP.

## Installation

```xml
<dependency>
    <groupId>com.mediadroppy</groupId>
    <artifactId>md-logging-java</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

Runtime dependencies are `log4j-core` and `com.rabbitmq:amqp-client` only — no Spring, no Jackson.

## Usage

### Via log4j2.xml

```xml
<Configuration>
  <Appenders>
    <MdLoggingAmqp name="mdLogging" application="md-user"
                   host="${env:RABBIT_HOST}" port="${env:RABBIT_PORT}"
                   username="${env:MD_USER_RABBIT_USERNAME}"
                   password="${env:MD_USER_RABBIT_PASSWORD}"/>
  </Appenders>
  <Loggers>
    <Root level="INFO">
      <AppenderRef ref="mdLogging"/>
    </Root>
  </Loggers>
</Configuration>
```

Then log as usual:

```java
private static final Logger log = LogManager.getLogger(UserService.class);

ThreadContext.put("x_forwarded_for", request.getHeader("X-Forwarded-For"));
log.info("user {} logged in", userId);
```

### Programmatically

```java
MdLogging.install(MdLoggingAmqpAppender.newBuilder()
        .setName("mdLogging")
        .setApplication("md-user")
        .setHost(System.getenv("RABBIT_HOST"))
        .setUsername(System.getenv("MD_USER_RABBIT_USERNAME"))
        .setPassword(System.getenv("MD_USER_RABBIT_PASSWORD"))
        .build());          // attaches to the root logger; MdLogging.uninstall("mdLogging") detaches
```

## Configuration reference

Only `name` and `application` are required.

| Attribute | Default | Meaning |
|---|---|---|
| `application` | — | Application name; becomes `application` in the envelope and the default routing key |
| `podName` | `$HOSTNAME`, else `unknown` | Pod identity in the envelope (Kubernetes sets `HOSTNAME` to the pod name) |
| `host` / `port` | `localhost` / `5672` | Broker address |
| `virtualHost` | `/` | AMQP virtual host |
| `username` / `password` | `guest` / `guest` | Credentials; use `${env:...}` lookups, never literals |
| `exchange` | `logging-events` | Durable topic exchange, declared on connect to match the agent |
| `routingKey` | the `application` value | The agent binds `#`, so any key reaches it |
| `declareExchange` | `true` | Set `false` if broker permissions forbid declaration |
| `queueCapacity` | `8192` | In-memory buffer of pending events |
| `maxEnvelopeBytes` | `1048576` | Agent's decoded-size limit; larger entries are replaced with a stub |
| `connectionTimeoutMillis` | `5000` | Broker connect timeout |
| `shutdownTimeoutMillis` | `2000` | Flush window on shutdown |
| `ignoreExceptions` | `true` | Standard Log4j semantics |

## Delivery semantics

- **Never blocks, never throws into the application.** Events are handed to a bounded queue and
  published by one background daemon thread.
- **Broker down:** the connection is lazy and retried with exponential backoff (200 ms → 30 s).
  Events buffer up to `queueCapacity`; beyond that they are dropped and counted
  (`droppedCount()`), with a rate-limited warning on Log4j's status logger.
- **Durability:** messages are published persistent (`deliveryMode=2`, `application/json`) to a
  durable exchange.
- **Oversized entries** are replaced with a small stub (keeping `level`/`logger`/`thread` and
  `truncated: true`) rather than being dead-lettered unseen by the agent.
- **Shutdown** flushes the queue for up to `shutdownTimeoutMillis`, then abandons the rest.

## Build and test

```bash
mvn clean verify        # JDK 21
```

The suite is self-contained except one round-trip test against a real RabbitMQ in Testcontainers,
which is skipped automatically when Docker is not detectable. With colima, expose the daemon
first:

```bash
export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

## CI

| Workflow | Trigger | What it does |
|---|---|---|
| `maven-test.yml` | called by the build workflows | JDK 21 (temurin, `cache: maven`), `mvn verify` |
| `pr-build.yml` | pull request to `master` | Runs the suite, attaches the built jar to the run |
| `main-build.yml` | push to `master` | Runs the suite, computes the next `X.Y.Z` from git tags, packages the versioned jar, uploads it, tags the commit |
