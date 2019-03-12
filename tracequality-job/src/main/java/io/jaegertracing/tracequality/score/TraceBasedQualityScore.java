package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.Span;

import java.util.List;
import java.util.Map;

/**
 * This score is computed on all spans belonging to a particular span.
 */
public interface TraceBasedQualityScore {
    /**
     * Computes trace quality score
     *
     * @param spanIdToSpans Map of SpanId to Spans. Spans are guaranteed to be from a single trace.
     *                      Do not modify this Map. TODO: Use an immutable map instead
     * @param out An aggregator for quality scores
     */
    void computeScore(Map<Long, List<Span>> spanIdToSpans, QualityScoreAggregator out);
}
