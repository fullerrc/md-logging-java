package com.mediadroppy.logging.client.log4j;

import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;

/**
 * Programmatic alternative to declaring the appender in {@code log4j2.xml}, for consumers that
 * assemble configuration in code (or cannot touch the XML they ship with):
 *
 * <pre>{@code
 * MdLogging.install(MdLoggingAmqpAppender.newBuilder()
 *         .setName("mdLogging")
 *         .setApplication("md-user")
 *         .setHost(System.getenv("RABBIT_HOST"))
 *         .build());
 * }</pre>
 */
public final class MdLogging {

    private MdLogging() {
    }

    /** Starts the appender and attaches it to the root logger of the current context. */
    public static void install(MdLoggingAmqpAppender appender) {
        Objects.requireNonNull(appender, "appender (a null appender usually means the builder "
                + "rejected its configuration — check the status log)");
        install(appender, (LoggerContext) LogManager.getContext(false));
    }

    static void install(MdLoggingAmqpAppender appender, LoggerContext context) {
        appender.start();
        Configuration configuration = context.getConfiguration();
        configuration.addAppender(appender);
        // Null level and filter: inherit whatever the root logger is configured to emit.
        configuration.getRootLogger().addAppender(appender, null, null);
        context.updateLoggers();
    }

    /** Detaches the named appender from the root logger and stops it. Unknown names are a no-op. */
    public static void uninstall(String appenderName) {
        uninstall(appenderName, (LoggerContext) LogManager.getContext(false));
    }

    static void uninstall(String appenderName, LoggerContext context) {
        Configuration configuration = context.getConfiguration();
        Appender appender = configuration.getAppenders().get(appenderName);
        configuration.getRootLogger().removeAppender(appenderName);
        context.updateLoggers();
        if (appender != null) {
            appender.stop();
        }
    }
}
