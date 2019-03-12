package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.QualityScore;

import java.util.*;

/**
 * This class caches and aggregates quality scores so that they can be emitted at once.
 */
public class QualityScoreAggregator {
    private final Map<String, QualityScore> qualityMetricCounts = new HashMap<>();

    private final Date dateBucket;

    public QualityScoreAggregator(Date dateBucket) {
        this.dateBucket = dateBucket;
    }


    public Optional<QualityScore> get(String service, String metric, String subMetric) {
        if (service == null || service.isEmpty() || dateBucket == null || metric == null || metric.isEmpty() || subMetric == null || subMetric.isEmpty()) {
            return Optional.empty();
        }
        String key = String.format("%s:%s:%s", service, metric, subMetric);
        //TODO: Collect spanId and TraceId in QualityScore aggregator
        return Optional.of(qualityMetricCounts.computeIfAbsent(key, l -> new QualityScore(dateBucket, service, metric, subMetric, 0L, "")));
    }

    public Optional<QualityScore> getPass(String service, String metric) {
        return get(service, metric, "PASS");
    }

    public Optional<QualityScore> getFail(String service, String metric) {
        return get(service, metric, "FAIL");
    }

    public Collection<QualityScore> getAll() {
        return qualityMetricCounts.values();
    }

}
