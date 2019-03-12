package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.QualityScore;
import io.jaegertracing.tracequality.model.Span;

import java.util.*;

/**
 * Holds the list of all trace quality scores to be computes and iteratively invokes them.
 */
public class QualityScoreProcessor {
    private final List<TraceBasedQualityScore> traceBasedQualityScores = new ArrayList<>();
    private final List<SpanBasedQualityScore> spanBasedQualityScores = new ArrayList<>();

    public void addTraceBasedQualityScore(TraceBasedQualityScore t) {
        traceBasedQualityScores.add(t);
    }

    public void addSpanBasedQualityScore(SpanBasedQualityScore s) {
        spanBasedQualityScores.add(s);
    }

    public Collection<QualityScore> compute(Iterable<Span> spans, Date dateBucket) {
        QualityScoreAggregator aggregator = new QualityScoreAggregator(dateBucket);

        Map<Long, List<Span>> spanIdToSpans = new HashMap<>();

        for (Span span : spans) {
            spanIdToSpans.computeIfAbsent(span.getSpanId(), l -> new ArrayList<>()).add(span);

            for (SpanBasedQualityScore spanBasedQualityScore : spanBasedQualityScores) {
                spanBasedQualityScore.computeScore(span, aggregator);
            }
        }

        spanIdToSpans = Collections.unmodifiableMap(spanIdToSpans);

        for (TraceBasedQualityScore traceBasedQualityScore : traceBasedQualityScores) {
            traceBasedQualityScore.computeScore(spanIdToSpans, aggregator);
        }
        return aggregator.getAll();
    }
}
