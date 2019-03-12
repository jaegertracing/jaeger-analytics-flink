package io.jaegertracing.analytics.query.resources;

import com.codahale.metrics.annotation.Timed;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import io.jaegertracing.analytics.query.model.tracequality.AggregatedQualityMetrics;
import io.jaegertracing.analytics.query.model.tracequality.JsonMetricSchema;
import io.jaegertracing.analytics.query.model.tracequality.JsonScoreSchema;
import io.jaegertracing.tracequality.model.QualityScore;

import javax.annotation.PostConstruct;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Path("/v1/qualitymetrics")
public class QualityMetricsResource {

    private static final String READ_QUERY = "SELECT count, service, metric, submetric, time_bucket, trace_id FROM quality_metrics WHERE service = ? AND time_bucket >= ?";

    @Context
    private Session session;
    private Mapper<QualityScore> qualityScoreMapper;

    @PostConstruct
    public void init() {
        MappingManager mappingManager = new MappingManager(session);
        qualityScoreMapper = mappingManager.mapper(QualityScore.class);
    }

    /**
     * Returns a sorted list of quality metrics with pass/fail counts and example traceIds.
     *
     * @param service       The service to retrieve quality metrics for
     * @param lookbackHours The number of hours to look back
     */
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public List<JsonMetricSchema> getMetrics(@QueryParam("service") String service, @QueryParam("hours") int lookbackHours) {
        Optional<AggregatedQualityMetrics> aggregatedQualityMetrics = getAggregatedQualityMetrics(service, lookbackHours);
        if (aggregatedQualityMetrics.isPresent()) {
            return aggregatedQualityMetrics.get().getMetrics();
        }
        return new ArrayList<>();
    }

    /**
     * Returns the average tracing score for completeness and quality metrics.
     *
     * @param service       The service to retrieve quality metrics for
     * @param lookbackHours The number of hours to look back
     */
    @GET
    @Timed
    @Path("/score")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonScoreSchema getScore(@QueryParam("service") String service, @QueryParam("hours") int lookbackHours) {
        Optional<AggregatedQualityMetrics> aggregatedQualityMetrics = getAggregatedQualityMetrics(service, lookbackHours);
        if (aggregatedQualityMetrics.isPresent()) {
            return aggregatedQualityMetrics.get().getScore();
        }
        return JsonScoreSchema.builder().build();
    }

    private Optional<AggregatedQualityMetrics> getAggregatedQualityMetrics(String service, int lookbackHours) {
        Date lookback = Date.from(Instant.now().minus(Duration.ofHours(lookbackHours)));
        ResultSet rs = session.execute(READ_QUERY, service, lookback);

        List<QualityScore> scores = qualityScoreMapper.map(rs).all();
        if (scores.isEmpty()) {
            return Optional.empty();
        }
        AggregatedQualityMetrics aggregatedQualityScore = new AggregatedQualityMetrics(scores.get(0).getService());
        for (QualityScore s : scores) {
            aggregatedQualityScore.addEntry(s.getMetric(), s.getSubmetric(), s.getCount(), s.getTraceId());
        }
        return Optional.of(aggregatedQualityScore);
    }
}
