package io.jaegertracing.tracequality.score;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;

import static io.jaegertracing.tracequality.score.HasClientServerSpans.HAS_CLIENT_SPANS;
import static io.jaegertracing.tracequality.score.HasClientServerSpans.HAS_SERVER_SPANS;
import static io.jaegertracing.tracequality.score.TestData.*;
import static io.jaegertracing.tracequality.score.TestData.FAIL;
import static io.jaegertracing.tracequality.score.TestData.PASS;
import static org.assertj.core.api.Assertions.tuple;

public class HasClientServerSpansTest {

    private final TestData testData = new TestData();
    private final QualityScoreAggregator aggregator = new QualityScoreAggregator(Date.from(Instant.ofEpochMilli(10000000)));
    private final HasClientServerSpans spansCheck = new HasClientServerSpans();

    @Test
    public void testServerSpansHappy() {
        testData.addSpan(1, 0, SpanKind.SERVER, "a", null);

        testData.addSpan(2, 1, SpanKind.CLIENT, "a", null);
        testData.addSpan(3, 1, SpanKind.CLIENT, "a", null);

        testData.addSpan(4, 2, SpanKind.SERVER, "b", null);
        testData.addSpan(5, 3, SpanKind.SERVER, "c", null);

        spansCheck.computeScore(testData.getSpanIdToSpans(), aggregator);

        Assertions.assertThat(aggregator.getAll())
                .extracting("service", "metric", "submetric", "count")
                .contains(tuple("a", HAS_SERVER_SPANS, PASS, 1L))
                .contains(tuple("a", HAS_CLIENT_SPANS, PASS, 2L))
                .contains(tuple("b", HAS_SERVER_SPANS, PASS, 1L))
                .contains(tuple("c", HAS_SERVER_SPANS, PASS, 1L));
    }

    @Test
    public void testFailServerRootSpanWithPeerService() {
        testData.addSpan(1, 0, SpanKind.SERVER, "a", "b");

        spansCheck.computeScore(testData.getSpanIdToSpans(), aggregator);

        Assertions.assertThat(aggregator.getAll())
                .extracting("service", "metric", "submetric", "count")
                .contains(tuple("a", HAS_SERVER_SPANS, PASS, 1L))
                .contains(tuple("b", HAS_CLIENT_SPANS, FAIL, 1L));
    }

    @Test
    public void testFailClientRootSpanWithPeerService() {
        testData.addSpan(1, 0, SpanKind.CLIENT, "a", "b");

        spansCheck.computeScore(testData.getSpanIdToSpans(), aggregator);

        Assertions.assertThat(aggregator.getAll())
                .extracting("service", "metric", "submetric", "count")
                .contains(tuple("a", HAS_CLIENT_SPANS, PASS, 1L))
                .contains(tuple("b", HAS_SERVER_SPANS, FAIL, 1L));
    }

    @Test
    public void testPassServerSpanWithNoChildren() {
        testData.addSpan(1, 0, SpanKind.SERVER, "a", null);

        spansCheck.computeScore(testData.getSpanIdToSpans(), aggregator);

        Assertions.assertThat(aggregator.getAll())
                .extracting("service", "metric", "submetric", "count")
                .contains(tuple("a", HAS_SERVER_SPANS, PASS, 1L));
    }

    @Test
    public void testPassClientSpanWithNoChildren() {
        testData.addSpan(1, 0, SpanKind.CLIENT, "a", null);

        spansCheck.computeScore(testData.getSpanIdToSpans(), aggregator);

        Assertions.assertThat(aggregator.getAll())
                .extracting("service", "metric", "submetric", "count")
                .contains(tuple("a", HAS_CLIENT_SPANS, PASS, 1L));
    }

    @Test
    public void testFailServerSpanHavingServerParent() {
        testData.addSpan(1, 0, SpanKind.SERVER, "a", null);
        testData.addSpan(2, 1, SpanKind.SERVER, "b", null);

        spansCheck.computeScore(testData.getSpanIdToSpans(), aggregator);

        Assertions.assertThat(aggregator.getAll())
                .extracting("service", "metric", "submetric", "count")
                .contains(tuple("b", HAS_CLIENT_SPANS, FAIL, 1L));
    }

    @Test
    public void testFailServerSpanHavingServerParentAttributeToPeerService() {
        testData.addSpan(1, 0, SpanKind.SERVER, "a", null);
        testData.addSpan(2, 1, SpanKind.SERVER, "b", "p");

        spansCheck.computeScore(testData.getSpanIdToSpans(), aggregator);

        Assertions.assertThat(aggregator.getAll())
                .extracting("service", "metric", "submetric", "count")
                .contains(tuple("p", HAS_CLIENT_SPANS, FAIL, 1L));

    }

    @Test
    public void testFailClientSpanHavingClientParent() {
        testData.addSpan(1, 0, SpanKind.CLIENT, "a", null);
        testData.addSpan(2, 1, SpanKind.CLIENT, "b", null);

        spansCheck.computeScore(testData.getSpanIdToSpans(), aggregator);

        Assertions.assertThat(aggregator.getAll())
                .extracting("service", "metric", "submetric", "count")
                .contains(tuple("b", HAS_SERVER_SPANS, FAIL, 1L));
    }
}
