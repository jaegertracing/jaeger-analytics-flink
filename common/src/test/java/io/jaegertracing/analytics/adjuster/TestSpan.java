package io.jaegertracing.analytics.adjuster;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
class TestSpan implements Dedupable {
    private long spanId;
    private long parentSpanId;
    private boolean isServer;
    private boolean isClient;
}
