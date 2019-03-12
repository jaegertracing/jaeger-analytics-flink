package io.jaegertracing.analytics.query.resources;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import io.jaegertracing.tracequality.model.QualityScore;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static javax.ws.rs.core.Response.Status.OK;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

public class QualityMetricsResourceIntegrationTest extends JerseyTest {

    private Mapper<QualityScore> qualityScoreMapper;
    private final Date bucket = Date.from(Instant.now().truncatedTo(ChronoUnit.HOURS));

    @Rule
    public final CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("tracequality.cql", "jaeger_tracequality_v1_local"));

    private void setup() {
        MappingManager mappingManager = new MappingManager(cassandraCQLUnit.session);
        qualityScoreMapper = mappingManager.mapper(QualityScore.class);
    }

    @Override
    protected Application configure() {
        ResourceConfig resourceConfig = new ResourceConfig(QualityMetricsResource.class);
        resourceConfig.register(new AbstractBinder() {
            @Override
            protected void configure() {
                setup();
                bind(cassandraCQLUnit.session).to(Session.class);
            }
        });

        return resourceConfig;
    }

    @Test
    public void testGetMetrics() {
        qualityScoreMapper.save(new QualityScore(bucket, "ueta", "HasServerSpans", "PASS", 100L, "123"));

        Response response = target("/v1/qualitymetrics")
                .queryParam("service", "ueta")
                .queryParam("hours", 1)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get();

        assertThat(response.getStatus()).isEqualTo(OK.getStatusCode());
        assertThatJson(response.readEntity(String.class))
                .isEqualTo("[{\"Service\":\"ueta\"," +
                        "\"Metric\":\"HasServerSpans\"," +
                        "\"Submetric\":\"PASS\"," +
                        "\"Description\":\"The service emitted spans with server span.kind\"," +
                        "\"Type\":\"Completeness\"," +
                        "\"Count\":100," +
                        "\"TraceIDs\":[\"123\"]}]");
    }

    @Test
    public void testGetScore() {
        qualityScoreMapper.save(new QualityScore(bucket, "fry", "HasServerSpans", "PASS", 100L, "123"));
        qualityScoreMapper.save(new QualityScore(bucket, "fry", "HasServerSpans", "FAIL", 100L, "123"));
        qualityScoreMapper.save(new QualityScore(bucket, "fry", "HasUniqueSpanIds", "PASS", 100L, "123"));
        qualityScoreMapper.save(new QualityScore(bucket, "fry", "HasUniqueSpanIds", "FAIL", 300L, "123"));

        Response response = target("/v1/qualitymetrics/score")
                .queryParam("service", "fry")
                .queryParam("hours", 1)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get();

        assertThat(response.getStatus()).isEqualTo(OK.getStatusCode());
        assertThatJson(response.readEntity(String.class))
                .isEqualTo("{\"Service\":\"fry\"," +
                        "\"TracingCompleteness\":0.5," +
                        "\"TracingQuality\":0.25}");
    }

}