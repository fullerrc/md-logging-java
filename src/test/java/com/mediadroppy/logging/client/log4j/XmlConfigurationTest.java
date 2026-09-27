package com.mediadroppy.logging.client.log4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.junit.jupiter.api.Test;

/**
 * Proves the plugin is discoverable the way a consumer will actually use it: named in
 * {@code log4j2.xml} with no {@code packages} attribute, which only works when the compile-time
 * plugin descriptor ({@code Log4j2Plugins.dat}) was generated. If this test fails with an
 * "unknown element" status error, annotation processing broke in the build.
 */
class XmlConfigurationTest {

    private static final String XML = """
            <?xml version="1.0" encoding="UTF-8"?>
            <Configuration status="ERROR">
              <Appenders>
                <MdLoggingAmqp name="mdLogging" application="xml-test-app" podName="pod-xml"
                               host="localhost" port="1"
                               connectionTimeoutMillis="200" shutdownTimeoutMillis="200"/>
              </Appenders>
              <Loggers>
                <Root level="INFO">
                  <AppenderRef ref="mdLogging"/>
                </Root>
              </Loggers>
            </Configuration>
            """;

    @Test
    void appenderIsDiscoverableFromXmlAndSurvivesAnUnreachableBroker() throws Exception {
        ConfigurationSource source =
                new ConfigurationSource(new ByteArrayInputStream(XML.getBytes(StandardCharsets.UTF_8)));
        LoggerContext context = new LoggerContext("xml-discovery-test");
        Configuration configuration = ConfigurationFactory.getInstance().getConfiguration(context, source);
        context.start(configuration);
        try {
            Appender appender = context.getConfiguration().getAppenders().get("mdLogging");
            assertThat(appender).isInstanceOf(MdLoggingAmqpAppender.class);
            assertThat(appender.isStarted()).isTrue();

            // Port 1 refuses connections: the append must neither throw nor block the caller.
            context.getLogger("xml.test").info("logged while the broker is unreachable");
        } finally {
            // A hung dispatcher would make this time out; prompt shutdown is part of the contract.
            assertThat(context.stop(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
