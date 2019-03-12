package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.score.TestData.SpanKind;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;

import static io.jaegertracing.tracequality.score.UniqueSpanId.HAS_UNIQUE_SPAN_IDS;
import static io.jaegertracing.tracequality.score.TestData.FAIL;
import static io.jaegertracing.tracequality.score.TestData.PASS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class UniqueSpanIdTest {
    private final QualityScoreAggregator aggregator = new QualityScoreAggregator(Date.from(Instant.ofEpochMilli(10000000)));
    final TestData testData = new TestData();
    final UniqueSpanId uniqueSpanId = new UniqueSpanId();

    @Test
    public void testFailSharedZipkinSpan() {
        testData.addSpan(1, 0, SpanKind.CLIENT, "a", null);
        testData.addSpan(1, 1, SpanKind.SERVER, "b", null);

        uniqueSpanId.computeScore(testData.getSpanIdToSpans(), aggregator);

        assertThat(aggregator.getAll())
                .extracting("service", "metric", "submetric", "count")
                .contains(tuple("a", HAS_UNIQUE_SPAN_IDS, FAIL, 1L))
                .contains(tuple("b", HAS_UNIQUE_SPAN_IDS, FAIL, 1L));
    }

    @Test
    public void testHappy() {
        testData.addSpan(1, 0, SpanKind.CLIENT, "a", null);
        testData.addSpan(2, 1, SpanKind.SERVER, "b", null);

        uniqueSpanId.computeScore(testData.getSpanIdToSpans(), aggregator);

        assertThat(aggregator.getAll())
                .extracting("service", "metric", "submetric", "count")
                .contains(tuple("a", HAS_UNIQUE_SPAN_IDS, PASS, 1L))
                .contains(tuple("b", HAS_UNIQUE_SPAN_IDS, PASS, 1L));
    }
}