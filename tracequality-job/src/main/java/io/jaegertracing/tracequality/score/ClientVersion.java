package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.QualityScore;
import io.jaegertracing.tracequality.model.Span;

/**
 * Emits the client version tag if it exists
 */
public class ClientVersion implements SpanBasedQualityScore {
    public static final String CLIENT_VERSION = "ClientVersion";

    @Override
    public void computeScore(Span span, QualityScoreAggregator out) {
        if (span.getClientVersion() != null && !span.getClientVersion().isEmpty()) {
            out.get(span.getServiceName(), CLIENT_VERSION, span.getClientVersion()).ifPresent(QualityScore::increment);
        }
    }
}
