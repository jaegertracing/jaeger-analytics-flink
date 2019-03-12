package io.jaegertracing.dependencies.cassandra;

import org.apache.flink.api.common.functions.AggregateFunction;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregates a stream of {@link Dependency} into {@link Dependencies}
 * by computing call counts.
 */
public class CassandraCallCountAggregator
        implements AggregateFunction<Dependency, Map<Dependency, Long>, Dependencies> {

    @Override
    public Map<Dependency, Long> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<Dependency, Long> add(Dependency link, Map<Dependency, Long> accumulator) {
        accumulator.merge(link, link.callCount, (x, y) -> x + y);
        return accumulator;
    }

    @Override
    public Dependencies getResult(Map<Dependency, Long> accumulator) {
        Dependencies result = new Dependencies(new ArrayList<>(),
                Date.from(Instant.now()),
                Date.from(Instant.now().truncatedTo(ChronoUnit.DAYS)));

        accumulator.forEach((k, v) -> result.getDependencies()
                .add(new Dependency(k.getParent(),
                        k.getChild(),
                        v)));
        return result;
    }

    @Override
    public Map<Dependency, Long> merge(Map<Dependency, Long> a, Map<Dependency, Long> b) {
        a.forEach((k, v) -> b.merge(k, v, (x, y) -> x + y));
        return b;
    }
}
