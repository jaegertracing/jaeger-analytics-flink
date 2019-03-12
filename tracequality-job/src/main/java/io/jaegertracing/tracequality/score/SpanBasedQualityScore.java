package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.Span;

/**
 * This score can be computed from a single span.
 */
interface SpanBasedQualityScore {
    void computeScore(Span span, QualityScoreAggregator out);
}
