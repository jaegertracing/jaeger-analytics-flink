package io.jaegertracing.analytics.query.uber;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import lombok.Getter;
import lombok.Setter;
import systems.composable.dropwizard.cassandra.CassandraFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class BootstrapConfiguration extends Configuration {
    @Valid
    @NotNull
    @Getter
    @Setter
    @JsonProperty("cassandra")
    private CassandraFactory cassandraFactory = new CassandraFactory();
}
