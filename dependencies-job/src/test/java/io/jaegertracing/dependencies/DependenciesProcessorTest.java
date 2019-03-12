package io.jaegertracing.dependencies;

import com.uber.jaeger.Process;
import com.uber.jaeger.Tag;
import io.jaegertracing.dependencies.cassandra.Dependencies;
import io.jaegertracing.dependencies.cassandra.Dependency;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DependenciesProcessorTest {

    private static final String KIND_SERVER = "server";
    private static final String KIND_CLIENT = "client";

    private final String[] services = {"Thorin", "Balin", "Bifur", "Bofur", "Bombur", "Dwalin", "Fili"};

    private StreamExecutionEnvironment env;

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    private com.uber.jaeger.Span createSpan(long traceId, long spanId, long parentSpanId, int serviceIdx, String spanKind) {
        com.uber.jaeger.Span span = new com.uber.jaeger.Span();
        span.setTraceIdLow(traceId);
        span.setTraceIdHigh(traceId);
        span.setSpanId(spanId);
        span.setParentSpanId(parentSpanId);
        span.setOperationName("operation");

        Tag tag = new Tag();
        tag.setKey("span.kind");
        tag.setVStr(spanKind);
        tag.setVType("vStr");
        span.setTags(Collections.singletonList(tag));

        Process process = new Process();
        process.setServiceName(services[serviceIdx]);
        span.setProcess(process);

        return span;
    }

    @Test
    public void testEndToEnd() throws Exception {

        com.uber.jaeger.Span[] spans = {
                createSpan(12, 5, 0, 1, KIND_SERVER),
                createSpan(12, 6, 5, 2, KIND_CLIENT),
                createSpan(12, 7, 5, 2, KIND_CLIENT),
                createSpan(12, 8, 5, 3, KIND_CLIENT),

                // Local span - should be ignored
                createSpan(12, 8, 5, 3, "local"),

                // Zipkin style spans with repeated spanids
                createSpan(50, 5, 0, 4, KIND_CLIENT),
                createSpan(50, 5, 0, 5, KIND_SERVER),
        };

        DataStreamSource<com.uber.jaeger.Span> source = env.fromElements(spans);

        DependenciesProcessor.setupJob(source.map(new SpanDeserializer()), new DependenciesSink());

        env.execute();

        Assertions.assertThat(DependenciesSink.values.get(0).getDependencies())
                .containsExactlyInAnyOrder(
                        new Dependency(services[1], services[2], 2L),
                        new Dependency(services[1], services[3], 1L),
                        new Dependency(services[4], services[5], 1L));

    }

    private static class DependenciesSink implements SinkFunction<Dependencies> {
        // This is static because flink serializes sinks
        public static final List<Dependencies> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Dependencies value, Context context) {
            values.add(value);
        }
    }
}


