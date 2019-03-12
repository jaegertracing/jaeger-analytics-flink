package io.jaegertracing.analytics.query.model.tracequality;

import lombok.Data;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@link AggregatedQualityMetrics} aggregates quality scores across multiple time buckets, and maintains them in a
 * sorted order. It also adds descriptions and types to the quality scores.
 */
@Value
public class AggregatedQualityMetrics {
    public static final String PASS = "PASS";
    public static final String FAIL = "FAIL";

    private final String service;
    // Use a sorted map so that ordering is stable when viewed in the UI
    private final Map<Key, CountTraceId> scoreMap = new TreeMap<>();

    public void addEntry(String metric, String submetric, Long count, String traceId) {
        scoreMap.compute(new Key(metric, submetric), (k, v) -> (v == null) ? new CountTraceId(count, traceId) : v.merge(count, traceId));
    }

    public List<JsonMetricSchema> getMetrics() {
        List<JsonMetricSchema> scores = new ArrayList<>(scoreMap.size());
        for (Map.Entry<Key, CountTraceId> entry : scoreMap.entrySet()) {
            DescriptionType descriptionType = DescriptionType.fromMetric(entry.getKey().metric);
            JsonMetricSchema item = JsonMetricSchema.builder()
                    .service(service)
                    .metric(entry.getKey().metric)
                    .submetric(entry.getKey().submetric)
                    .count(entry.getValue().count)
                    .traceIds(entry.getValue().getTraceIds())
                    .description(descriptionType.getDescription())
                    .type(descriptionType.getType().getType()).build();
            scores.add(item);
        }
        return scores;
    }

    public JsonScoreSchema getScore() {
        float qualityPassCount = computeSum(DescriptionType.Type.QUALITY, PASS);
        float qualityFailCount = computeSum(DescriptionType.Type.QUALITY, FAIL);
        float qualityTotal = qualityPassCount + qualityFailCount;

        float completenessPassCount = computeSum(DescriptionType.Type.COMPLETENESS, PASS);
        float completenessFailCount = computeSum(DescriptionType.Type.COMPLETENESS, FAIL);
        float completenessTotal = completenessPassCount + completenessFailCount;


        JsonScoreSchema.JsonScoreSchemaBuilder scoreBuilder = JsonScoreSchema.builder().service(this.service);

        if (qualityTotal != 0) {
            scoreBuilder.tracingQuality(qualityPassCount / qualityTotal);

        }

        if (completenessTotal != 0) {
            scoreBuilder.tracingCompleteness(completenessPassCount / completenessTotal);
        }

        return scoreBuilder.build();
    }

    private float computeSum(DescriptionType.Type type, String submetric) {
        float total = 0;
        for (Map.Entry<Key, CountTraceId> entry : scoreMap.entrySet()) {
            if (DescriptionType.fromMetric(entry.getKey().metric).getType().equals(type) && entry.getKey().submetric.equalsIgnoreCase(submetric)) {
                total += entry.getValue().count;
            }
        }
        return total;
    }

    @Value
    private class Key implements Comparable<Key> {
        private final String metric;
        private final String submetric;

        @Override
        public int compareTo(Key that) {
            int compareMetric = String.CASE_INSENSITIVE_ORDER.compare(this.metric, that.metric);
            if (compareMetric != 0) {
                return compareMetric;
            }
            return String.CASE_INSENSITIVE_ORDER.compare(this.submetric, that.submetric);
        }
    }

    @Data
    private class CountTraceId {
        private Long count;
        private final List<String> traceIds = new ArrayList<>();

        CountTraceId(long count, String traceId) {
            this.count = count;
            traceIds.add(traceId);
        }

        CountTraceId merge(long count, String traceId) {
            this.count += count;
            this.traceIds.add(traceId);
            return this;
        }
    }
}
