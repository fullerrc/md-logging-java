package com.mediadroppy.logging.client.log4j;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.mediadroppy.logging.client.support.RecordingPublisher;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class MdLoggingTest {

    private final RecordingPublisher publisher = new RecordingPublisher();
    private final LoggerContext context = new LoggerContext("md-logging-install-test");

    @AfterEach
    void tearDown() {
        context.stop(5, SECONDS);
    }

    @Test
    void installAttachesToTheRootLoggerAndUninstallDetaches() {
        context.start();
        MdLoggingAmqpAppender appender = MdLoggingAmqpAppender.newBuilder()
                .setPublisherFactory(config -> publisher)
                .setName("md")
                .setApplication("md-user")
                .build();

        MdLogging.install(appender, context);

        assertThat(appender.isStarted()).isTrue();
        context.getLogger("some.consumer.Code").error("something failed");
        await().atMost(5, SECONDS).until(() -> publisher.published().size() == 1);

        MdLogging.uninstall("md", context);

        assertThat(appender.isStopped()).isTrue();
        context.getLogger("some.consumer.Code").error("after uninstall");
        assertThat(publisher.published()).hasSize(1);
    }

    @Test
    void uninstallOfAnUnknownNameIsANoOp() {
        context.start();
        MdLogging.uninstall("never-installed", context);
    }
}
