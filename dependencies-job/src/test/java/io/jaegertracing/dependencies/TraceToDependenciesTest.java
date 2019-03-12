package io.jaegertracing.dependencies;

import io.jaegertracing.dependencies.cassandra.Dependency;
import io.jaegertracing.dependencies.model.Span;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class TraceToDependenciesTest {

    private static final String[] SERVICE = {"Ada", "Batata vada", "Bonda", "Boondi", "Dhokla"};

    private Span makeSpan(long traceId, long spanId, long parentSpanId, String serviceName) {
        Span span = new Span();
        span.setTraceIdLow(traceId);
        span.setSpanId(spanId);
        span.setParentSpanId(parentSpanId);
        span.setServiceName(serviceName);
        return span;
    }

    @Test
    public void testNoDependency() throws Exception {
        List<Span> spans = new ArrayList<>();
        spans.add(makeSpan(1, 1, 0, SERVICE[0]));


        TraceToDependencies traceToDependencies = new TraceToDependencies();
        List<Dependency> dependencies = new ArrayList<>();
        traceToDependencies.flatMap(spans, new ListCollector<>(dependencies));
        assertThat(dependencies).isEmpty();
    }

    /**
     * Service calling itself.
     *  a
     *  |
     *  a
     */
    @Test
    public void testCircularDependencies() throws Exception {
        List<Span> spans = new ArrayList<>();
        spans.add(makeSpan(1, 1, 0, SERVICE[0]));
        spans.add(makeSpan(1, 2, 1, SERVICE[0]));

        TraceToDependencies traceToDependencies = new TraceToDependencies();
        List<Dependency> dependencies = new ArrayList<>();
        traceToDependencies.flatMap(spans, new ListCollector<>(dependencies));
        assertThat(dependencies).containsExactly(new Dependency(SERVICE[0], SERVICE[0], (long) 1));
    }

    /**
     * Service calling another service twice.
     *     a
     *    / \
     *   b  b
     */
    @Test
    public void testOneDependencies() throws Exception {
        List<Span> spans = new ArrayList<>();
        spans.add(makeSpan(1, 1, 0, SERVICE[0]));

        spans.add(makeSpan(1, 2, 1, SERVICE[1]));
        spans.add(makeSpan(1, 3, 1, SERVICE[1]));

        TraceToDependencies traceToDependencies = new TraceToDependencies();
        List<Dependency> dependencies = new ArrayList<>();
        traceToDependencies.flatMap(spans, new ListCollector<>(dependencies));
        assertThat(dependencies)
                .containsExactly(
                        new Dependency(SERVICE[0], SERVICE[1], (long) 1), new Dependency(SERVICE[0], SERVICE[1], (long) 1));
    }

    /**
     * Service calling two services.
     *    a
     *   / \
     *  b  c
     */
    @Test
    public void testTwoDependencies() throws Exception {
        List<Span> spans = new ArrayList<>();
        spans.add(makeSpan(1, 1, 0, SERVICE[0]));

        spans.add(makeSpan(1, 2, 1, SERVICE[1]));
        spans.add(makeSpan(1, 3, 1, SERVICE[2]));

        TraceToDependencies traceToDependencies = new TraceToDependencies();
        List<Dependency> dependencies = new ArrayList<>();
        traceToDependencies.flatMap(spans, new ListCollector<>(dependencies));
        assertThat(dependencies)
                .extracting(Dependency::getParent, Dependency::getChild)
                .contains(tuple(SERVICE[0], SERVICE[1]), tuple(SERVICE[0], SERVICE[2]));
    }

    /**
     * Service calling multiple services.
     *            a
     *           / \
     *          b  c
     *         /
     *        d
     *       / \
     *      e  a
     *          \
     *          c
     */
    @Test
    public void testMultipleDependencies() throws Exception {
        List<Span> spans = new ArrayList<>();
        spans.add(makeSpan(1, 1, 0, SERVICE[0])); // a

        spans.add(makeSpan(1, 2, 1, SERVICE[1])); // a-b
        spans.add(makeSpan(1, 3, 1, SERVICE[2])); // a-c

        spans.add(makeSpan(1, 4, 2, SERVICE[3])); // b-d

        spans.add(makeSpan(1, 5, 4, SERVICE[4])); // d-e
        spans.add(makeSpan(1, 6, 4, SERVICE[0])); // d-a

        spans.add(makeSpan(1, 7, 6, SERVICE[2])); // a-c

        TraceToDependencies traceToDependencies = new TraceToDependencies();
        List<Dependency> dependencies = new ArrayList<>();
        traceToDependencies.flatMap(spans, new ListCollector<>(dependencies));
        assertThat(dependencies)
                .extracting(Dependency::getParent, Dependency::getChild)
                .contains(
                        tuple(SERVICE[0], SERVICE[1]), // a-b
                        tuple(SERVICE[0], SERVICE[2]), // a-c
                        tuple(SERVICE[0], SERVICE[2]), // a-c
                        tuple(SERVICE[1], SERVICE[3]), // b-d
                        tuple(SERVICE[3], SERVICE[4]), // d-e
                        tuple(SERVICE[3], SERVICE[0]));// d-a
    }
}
