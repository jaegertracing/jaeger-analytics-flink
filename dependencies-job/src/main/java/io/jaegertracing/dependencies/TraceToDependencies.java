package io.jaegertracing.dependencies;

import io.jaegertracing.dependencies.cassandra.Dependency;
import io.jaegertracing.dependencies.model.Span;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Converts a {@link Iterable<Span>} into {@link Dependency} following the hierarchy
 * defined by parent spanId.
 * Note that each call is represented as a separate {@link Dependency}.
 */
public class TraceToDependencies extends RichFlatMapFunction<Iterable<Span>, Dependency> {
    @Override
    public void flatMap(Iterable<Span> spans, Collector<Dependency> dependencies) {
        Map<Long, String> spanIdToService = new HashMap<>();
        spans.forEach(span -> spanIdToService.put(span.getSpanId(), span.getServiceName()));

        for (Span span : spans) {
            String parent = spanIdToService.get(span.getParentSpanId());
            String child = span.getServiceName();
            if (parent == null) {
                continue;
            }

            dependencies.collect(new Dependency(parent, child, (long) 1));
        }
    }
}
