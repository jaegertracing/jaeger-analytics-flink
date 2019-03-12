package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.Span;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;


@RunWith(MockitoJUnitRunner.class)
public class QualityScoreProcessorTest {
    @Mock SpanBasedQualityScore spanBasedQualityScore;
    @Mock TraceBasedQualityScore traceBasedQualityScore;
    @Captor ArgumentCaptor<Map<Long, List<Span>>> spanIdToSpans;

    private final Span span1 = new Span();
    private final Span span2 = new Span();
    private final Span span3 = new Span();

    private final List<Span> spans = new ArrayList<>();
    private final Date date = Date.from(Instant.ofEpochMilli(10000));

    @Before
    public void setup() {
        span1.setSpanId(1);
        span2.setSpanId(2);
        span3.setSpanId(2); //duplicate spanId

    }

    @Test
    public void testSpanBasedQualityScore() {
        QualityScoreProcessor qualityScoreProcessor = new QualityScoreProcessor();
        qualityScoreProcessor.addSpanBasedQualityScore(spanBasedQualityScore);

        spans.add(span1);
        spans.add(span2);

        qualityScoreProcessor.compute(spans, date);

        verify(spanBasedQualityScore).computeScore(eq(span1), any());
        verify(spanBasedQualityScore).computeScore(eq(span2), any());
    }

    @Test
    public void testTraceBasedQualityScore() {
        QualityScoreProcessor qualityScoreProcessor = new QualityScoreProcessor();
        qualityScoreProcessor.addTraceBasedQualityScore(traceBasedQualityScore);

        spans.add(span1);
        spans.add(span2);
        spans.add(span3);

        qualityScoreProcessor.compute(spans, date);


        verify(traceBasedQualityScore).computeScore(spanIdToSpans.capture(), any());
        assertThat(spanIdToSpans.getValue())
                .containsOnlyKeys(span1.getSpanId(), span2.getSpanId());
        assertThat(spanIdToSpans.getValue().get(span1.getSpanId())).containsExactly(span1);
        assertThat(spanIdToSpans.getValue().get(span2.getSpanId())).containsExactly(span2, span3);
    }
}