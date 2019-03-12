package io.jaegertracing.analytics.config;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Properties;

import static org.mockito.Mockito.mock;

public class ConfigurationUtilsTest {

    @Test
    public void testFilterPrefix(){
        Properties input = new Properties();
        input.setProperty("kafka.bootstrap.servers", "klok123-dca1:9092");

        Properties output = Utils.filterPrefix(input, "kafka");
        Assertions.assertThat(output.getProperty("bootstrap.servers")).isEqualTo("klok123-dca1:9092");
    }
}