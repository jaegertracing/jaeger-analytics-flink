package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.QualityScore;
import io.jaegertracing.tracequality.model.Span;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;

import static io.jaegertracing.tracequality.score.MinimumClientVersion.MIN_VERSION_CHECK;
import static io.jaegertracing.tracequality.score.TestData.FAIL;
import static io.jaegertracing.tracequality.score.TestData.PASS;
import static org.assertj.core.api.Assertions.assertThat;

public class MinimumClientVersionTest {
    private static final String SERVICE_NAME = "fubar";
    private final Span span = new Span();
    private final MinimumClientVersion minimumClientVersion = new MinimumClientVersion();
    private final Date bucket = Date.from(Instant.ofEpochSecond(100000000));

    @Before
    public void setup() {
        span.setServiceName(SERVICE_NAME);
    }

    @Test
    public void testMinClientVersionCheckFail() {
        String[] failVersions = {"Go-2.5.9", "java-0.0.16", "python-3.4.0", "node-2.1.1", "", "go-", "5.5.5", null};
        iterateAndAssert(failVersions, FAIL);
    }

    @Test
    public void testMinClientVersionCheckPass() {
        String[] passVersions = {"Go-2.6.1", "java-0.2.18", "python-3.9.0", "node-3.1.1"};
        iterateAndAssert(passVersions, PASS);
    }

    private void iterateAndAssert(String[] versions, String status) {
        for (String version : versions) {
            QualityScoreAggregator aggregator = new QualityScoreAggregator(bucket);
            span.setClientVersion(version);
            minimumClientVersion.computeScore(span, aggregator);
            assertThat(aggregator.getAll()).containsOnly(new QualityScore(bucket, SERVICE_NAME, MIN_VERSION_CHECK, status, 1L, ""));
        }
    }

    @Test
    public void testIsGreaterThanOrEqualTo() {
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3", "1.2.4")).isTrue();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3", "1.3.3")).isTrue();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3", "2.2.3")).isTrue();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3", "1.3")).isTrue();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3", "3")).isTrue();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.1", "3")).isTrue();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1", "3")).isTrue();

        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3", "1.2.3")).isTrue();

        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3", "1.2.2")).isFalse();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3", "1.1.3")).isFalse();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3", "0.2.3")).isFalse();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3", "0.2")).isFalse();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3", "0")).isFalse();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2", "1.1.5")).isFalse();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1", "0.0.5")).isFalse();

        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3", "1.2.3dev")).isFalse();
        assertThat(minimumClientVersion.isGreaterThanOrEqualTo("1.2.3dev", "1.2.3dev")).isFalse();
    }
}