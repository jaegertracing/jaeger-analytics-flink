package io.jaegertracing.analytics.query.model.tracequality;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonNaming(value = PropertyNamingStrategy.UpperCamelCaseStrategy.class)
public class JsonScoreSchema {
    String service;
    Float tracingCompleteness;
    Float tracingQuality;
}
