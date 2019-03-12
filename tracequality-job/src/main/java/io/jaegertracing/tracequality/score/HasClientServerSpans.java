package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.QualityScore;
import io.jaegertracing.tracequality.model.Span;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HasClientServerSpans implements TraceBasedQualityScore {
    public static final String HAS_CLIENT_SPANS = "HasClientSpans";
    public static final String HAS_SERVER_SPANS = "HasServerSpans";

    @Override
    public void computeScore(Map<Long, List<Span>> spanIdToSpans, QualityScoreAggregator aggregator) {
        Map<Long, List<Long>> spanIdToChildren = new HashMap<>();

        spanIdToSpans.forEach((spanId, spans) -> spans.forEach(span -> spanIdToChildren.computeIfAbsent(span.getParentSpanId(), l -> new ArrayList<>()).add(span.getSpanId())));

        for (Map.Entry<Long, List<Long>> spanToChildren : spanIdToChildren.entrySet()) {
            List<Span> potentialParents = spanIdToSpans.get(spanToChildren.getKey());
            Span parent = potentialParents == null || potentialParents.isEmpty() ? null : potentialParents.get(0);

            List<Span> children = spanToChildren.getValue().stream().map(key -> spanIdToSpans.get(key).get(0)).collect(Collectors.toList());

            if (parent == null) { // Root Span
                for (Span span : children) {
                    if (span.isClient()) {
                        aggregator.getFail(span.getPeerService(), HAS_SERVER_SPANS).ifPresent(QualityScore::increment);
                        aggregator.getPass(span.getServiceName(), HAS_CLIENT_SPANS).ifPresent(QualityScore::increment);
                    } else if (span.isServer()) {
                        aggregator.getFail(span.getPeerService(), HAS_CLIENT_SPANS).ifPresent(QualityScore::increment);
                        aggregator.getPass(span.getServiceName(), HAS_SERVER_SPANS).ifPresent(QualityScore::increment);
                    }
                }
                continue;
            }

            if (parent.isClient()) {
                children.stream().filter(Span::isServer).forEach(span -> {
                    aggregator.getPass(span.getServiceName(), HAS_SERVER_SPANS).ifPresent(QualityScore::increment);
                });

                // Shouldn't have client span being nested in client span
                // TODO: research if this can happen in middleware
                children.stream().filter(Span::isClient).forEach(span -> {
                    aggregator.getFail(span.getServiceName(), HAS_SERVER_SPANS).ifPresent(QualityScore::increment);
                });
            } else if (parent.isServer()) {
                children.stream().filter(Span::isClient).forEach(span -> {
                    aggregator.getPass(span.getServiceName(), HAS_CLIENT_SPANS).ifPresent(QualityScore::increment);
                });

                // Shouldn't have server span nesting another server span
                children.stream().filter(Span::isServer).forEach(span -> {
                    if (span.getPeerService() != null) {
                        aggregator.getFail(span.getPeerService(), HAS_CLIENT_SPANS).ifPresent(QualityScore::increment);
                    } else {
                        aggregator.getFail(span.getServiceName(), HAS_CLIENT_SPANS).ifPresent(QualityScore::increment);
                    }
                });
            }
        }

    }
}
