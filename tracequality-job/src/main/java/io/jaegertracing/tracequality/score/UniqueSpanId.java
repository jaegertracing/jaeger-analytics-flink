package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.QualityScore;
import io.jaegertracing.tracequality.model.Span;

import java.util.List;
import java.util.Map;

/**
 * Emits a failing score if there are duplicate spanIds in the same trace.
 */
public class UniqueSpanId implements TraceBasedQualityScore {
    public static final String HAS_UNIQUE_SPAN_IDS = "HasUniqueSpanIds";

    @Override
    public void computeScore(Map<Long, List<Span>> spanIdToSpans, QualityScoreAggregator out) {
        for (List<Span> spans : spanIdToSpans.values()) {
            if (spans.size() > 1) {
                spans.forEach(s -> out.getFail(s.getServiceName(), HAS_UNIQUE_SPAN_IDS).ifPresent(QualityScore::increment));
            } else {
                out.getPass(spans.get(0).getServiceName(), HAS_UNIQUE_SPAN_IDS).ifPresent(QualityScore::increment);
            }
        }
    }
}
