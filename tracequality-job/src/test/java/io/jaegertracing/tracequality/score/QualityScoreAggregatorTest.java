package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.QualityScore;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class QualityScoreAggregatorTest {

    private final Date date = Date.from(Instant.ofEpochMilli(1000000));
    private final QualityScoreAggregator aggregator = new QualityScoreAggregator(date);

    @Test
    public void testCacheMetric() {
        Optional<QualityScore> qualityScore1 = aggregator.get("a", "b", "c");
        Optional<QualityScore> qualityScore2 = aggregator.get("a", "b", "c");

        assertThat(qualityScore1).isEqualTo(qualityScore2);
        assertThat(aggregator.getAll()).containsOnly(new QualityScore(date, "a", "b", "c", 0L, ""));
    }

    @Test
    public void testIncrement() {
        aggregator.get("a", "b", "c").get().increment();
        aggregator.get("a", "b", "d").get().increment();

        assertThat(aggregator.getAll()).containsOnly(
                new QualityScore(date, "a", "b", "c", 1L, ""),
                new QualityScore(date, "a", "b", "d", 1L, "")
        );
    }

    @Test
    public void testGetPass() {
        aggregator.getPass("a", "b");
        assertThat(aggregator.getAll()).containsOnly(new QualityScore(date, "a", "b", "PASS", 0L, ""));
    }

    @Test
    public void testGetFail() {
        aggregator.getFail("a", "b");
        assertThat(aggregator.getAll()).containsOnly(new QualityScore(date, "a", "b", "FAIL", 0L, ""));
    }
}