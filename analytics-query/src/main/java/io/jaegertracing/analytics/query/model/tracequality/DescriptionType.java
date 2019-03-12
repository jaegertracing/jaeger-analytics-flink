package io.jaegertracing.analytics.query.model.tracequality;

import lombok.Getter;

public enum DescriptionType {
    HAS_SERVER_SPANS("HasServerSpans", Type.COMPLETENESS, "The service emitted spans with server span.kind"),
    HAS_CLIENT_SPANS("HasClientSpans", Type.COMPLETENESS, "The service emitted spans with client span.kind "),
    HAS_UNIQUE_SPAN_IDS("HasUniqueSpanIds", Type.QUALITY, "The service emitted spans with unique span ids"),
    MINIMUM_CLIENT_VERSION_CHECK("MinimumClientVersionCheck", Type.COMPLETENESS, "This service emitted a span that has an acceptable client version"),
    OTHER("Unknown", Type.OTHER, "This metric isn't used for computing trace quality");

    @Getter
    private final String name;
    @Getter
    private final String description;
    @Getter
    private final Type type;

    DescriptionType(String name, Type type, String description) {
        this.name = name;
        this.type = type;
        this.description = description;
    }

    // See https://docs.google.com/document/d/1_P6uTyvErMWMOhNa5B22g08WP9nFpN75XrTlhDXJiks/ for details
    enum Type {
        COMPLETENESS("Completeness"), // Doesn't break context propagation
        QUALITY("Quality"), // Spans contain good data
        OTHER("Other");

        @Getter
        private final String type;

        Type(String type) {
            this.type = type;
        }
    }

    public static DescriptionType fromMetric(String metric) {
        for (DescriptionType value : DescriptionType.values()) {
            if (metric.equalsIgnoreCase(value.name)) {
                return value;
            }
        }
        return OTHER;
    }
}
