package io.jaegertracing.dependencies;

import io.jaegertracing.analytics.JaegerJob;
import com.uber.jaeger.Span;
import io.jaegertracing.dependencies.cassandra.Dependencies;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

@Slf4j
public class DependenciesJob implements JaegerJob<Dependencies> {
    /**
     * These constants are used to provide user friendly names for Flink operators. Flink also uses them in
     * metric names.
     */
    private static final String DESERIALIZE_SPAN = "DeserializeSpan";

    public static void main(String[] args) throws Exception {
        DependenciesJob job = new DependenciesJob();
        job.executeJob("Dependencies Job", TypeInformation.of(Dependencies.class));
    }

    @Override
    public void setupJob(ParameterTool parameterTool, DataStream<Span> spans, SinkFunction<Dependencies> sinkFunction) {
        SingleOutputStreamOperator<io.jaegertracing.dependencies.model.Span> modelSpans = spans.map(new SpanDeserializer()).name(DESERIALIZE_SPAN);
        DependenciesProcessor.setupJob(modelSpans, sinkFunction);
    }
}
