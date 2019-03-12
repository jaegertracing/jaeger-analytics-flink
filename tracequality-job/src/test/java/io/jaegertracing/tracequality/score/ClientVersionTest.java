package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.Span;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;

import static io.jaegertracing.tracequality.score.ClientVersion.CLIENT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class ClientVersionTest {
    private final QualityScoreAggregator aggregator = new QualityScoreAggregator(Date.from(Instant.ofEpochMilli(10000000)));
    final ClientVersion clientVersion = new ClientVersion();

    @Test
    public void testIsGreaterThanOrEqualTo() {
        Span span = new Span();
        span.setServiceName("boop");
        span.setClientVersion("version-string");

        clientVersion.computeScore(span, aggregator);

        assertThat(aggregator.getAll())
                .extracting("service", "metric", "submetric", "count")
                .contains(tuple("boop", CLIENT_VERSION, "version-string", 1L));

    }

}