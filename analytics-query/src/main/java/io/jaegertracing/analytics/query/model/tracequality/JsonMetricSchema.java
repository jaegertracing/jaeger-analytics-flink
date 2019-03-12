package io.jaegertracing.analytics.query.model.tracequality;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder
@JsonNaming(value = PropertyNamingStrategy.UpperCamelCaseStrategy.class)
public class JsonMetricSchema {
    final String service;
    final String metric;
    final String submetric;
    final String description;
    final String type;
    final Long count;
    @JsonProperty("TraceIDs")
    final List<String> traceIds;
}
