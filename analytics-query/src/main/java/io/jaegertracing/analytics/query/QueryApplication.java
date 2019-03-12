package io.jaegertracing.analytics.query;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.jaegertracing.analytics.query.resources.HealthResource;
import io.jaegertracing.analytics.query.resources.QualityMetricsResource;
import io.jaegertracing.analytics.query.uber.BootstrapConfiguration;
import lombok.extern.slf4j.Slf4j;
import systems.composable.dropwizard.cassandra.CassandraBundle;
import systems.composable.dropwizard.cassandra.CassandraFactory;

@Slf4j
public class QueryApplication extends Application<BootstrapConfiguration> {
    public static void main(String[] args) throws Exception {
        new QueryApplication().run(args);
    }

    @Override
    public String getName() {
        return "jaeger-analytics-query";
    }

    @Override
    public void initialize(final Bootstrap<BootstrapConfiguration> bootstrap) {
        super.initialize(bootstrap);

        bootstrap.addBundle(new CassandraBundle<BootstrapConfiguration>() {
            @Override
            public CassandraFactory getCassandraFactory(BootstrapConfiguration configuration) {
                return configuration.getCassandraFactory();
            }
        });
    }

    @Override
    public void run(BootstrapConfiguration configuration, Environment environment) throws Exception {
        log.info("Starting jaeger-analytics-query");
        environment.jersey().register(HealthResource.class);
        environment.jersey().register(QualityMetricsResource.class);
    }
}
