package io.jaegertracing.dependencies;

import io.jaegertracing.analytics.JaegerJob;
import io.jaegertracing.dependencies.model.Span;
import io.jaegertracing.depenencies.model.DependencyPath;
import io.jaegertracing.depenencies.model.KeyValue;
import io.jaegertracing.depenencies.model.ServiceOperation;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DeepDependenciesJob implements JaegerJob<DependencyPath> {

  private static final Time traceCompletionTime = Time.minutes(5);
  private static final Time sinkAggregationTime = Time.minutes(10);

  public static void main(String[] args) throws Exception {
    DeepDependenciesJob job = new DeepDependenciesJob();
    job.executeJob("Deep Dependencies Job", TypeInformation.of(new TypeHint<DependencyPath>() {}));
  }

  @Override
  public void setupJob(
      ParameterTool parameterTool,
      DataStream<com.uber.jaeger.Span> spans,
      SinkFunction<DependencyPath> sinkFunction)
      throws Exception {

    SingleOutputStreamOperator<Tuple2<String, Iterable<Span>>> traces = batchSpansToTraces(spans);
    SingleOutputStreamOperator<DependencyPath> dependencyPaths = computeDependencyPaths(traces);
    SingleOutputStreamOperator<DependencyPath> focalNodeIndexes =
        computeIndexesForFocalNode(dependencyPaths);
    deduplicateAndWrite(focalNodeIndexes, sinkFunction);
  }

  /** Converts spans into an iterable of spans sharing the same traceId by using a session window */
  private SingleOutputStreamOperator<Tuple2<String, Iterable<Span>>> batchSpansToTraces(
      DataStream<com.uber.jaeger.Span> spans) {
    return spans
        .map(new SpanDeserializer())
        .name("DeserializeSpan")
        .keyBy(Span::getTraceIdLow)
        .window(EventTimeSessionWindows.withGap(traceCompletionTime))
        .apply(new SpanToTraceWindowFunction())
        .name("BatchSpansToTraces");
  }

  /** Computes all paths from root node to leaves and deduplicates them */
  private SingleOutputStreamOperator<DependencyPath> computeDependencyPaths(
      SingleOutputStreamOperator<Tuple2<String, Iterable<Span>>> traces) {
    return traces
        .flatMap(new TraceToDependencyPaths())
        .name("TraceToDependencies")
        .keyBy(DependencyPath::getPath)
        .timeWindow(sinkAggregationTime)
        .reduce((v1, v2) -> v1)
        .name("DeduplicatePaths");
  }

  /** See {@link IndexMapper} */
  private SingleOutputStreamOperator<DependencyPath> computeIndexesForFocalNode(
      SingleOutputStreamOperator<DependencyPath> dependencyPaths) {
    return dependencyPaths.flatMap(new IndexMapper()).name("GenerateIndexes");
  }

  /** Aggregate indexes using a time bucket and write to sink */
  private void deduplicateAndWrite(
      SingleOutputStreamOperator<DependencyPath> b, SinkFunction<DependencyPath> sinkFunction) {
    b.keyBy(v -> v)
        .timeWindow(sinkAggregationTime)
        .reduce((v1, v2) -> v1, new ComputeTimeBuckets())
        .name("DeduplicateWrites")
        .map(new AssignTimeBuckets())
        .name("AssignTimeBuckets")
        .addSink(sinkFunction)
        .name("Sink");
  }

  private static class SpanToTraceWindowFunction
      extends RichWindowFunction<Span, Tuple2<String, Iterable<Span>>, Long, TimeWindow> {

    @Override
    public void apply(
        Long traceId,
        TimeWindow window,
        Iterable<Span> input,
        Collector<Tuple2<String, Iterable<Span>>> out)
        throws Exception {
      out.collect(Tuple2.of(Long.toHexString(traceId), input));
    }
  }

  /**
   * IndexMapper converts a {@link DependencyPath} and emits one per every service/operationName
   * combination present in the DependenciesPath.
   */
  private static class IndexMapper implements FlatMapFunction<DependencyPath, DependencyPath> {

    @Override
    public void flatMap(DependencyPath value, Collector<DependencyPath> out) throws Exception {
      for (ServiceOperation serviceOperation : value.getPath()) {
        // emit path for service_operation pair
        emitPath(serviceOperation, value.getPath(), value.getAttributes(), out);
        // emit path for the service so that we can query the path by only serviceName
        emitPath(
            new ServiceOperation(serviceOperation.getService(), ServiceOperation.ALL_OPERATIONS),
            value.getPath(),
            value.getAttributes(),
            out);
      }
    }

    private void emitPath(
        ServiceOperation focalNode,
        List<ServiceOperation> path,
        List<KeyValue> attributes,
        Collector<DependencyPath> out) {
      DependencyPath dependencyPath = new DependencyPath();
      dependencyPath.setFocalNode(focalNode);
      dependencyPath.setPath(path);
      dependencyPath.setAttributes(attributes);
      out.collect(dependencyPath);
    }
  }

  /** BucketAssigner extracts the time window bucket into a Tuple */
  private static class ComputeTimeBuckets
      implements WindowFunction<
          DependencyPath, Tuple2<Date, DependencyPath>, DependencyPath, TimeWindow> {

    @Override
    public void apply(
        DependencyPath dependencyPath,
        TimeWindow window,
        Iterable<DependencyPath> input,
        Collector<Tuple2<Date, DependencyPath>> out)
        throws Exception {
      for (DependencyPath path : input) {
        Instant windowEnd = Instant.ofEpochMilli(window.getEnd());
        Date bucket = Date.from(windowEnd.truncatedTo(ChronoUnit.HOURS));
        out.collect(Tuple2.of(bucket, path));
      }
    }
  }

  private static class AssignTimeBuckets
      implements MapFunction<Tuple2<Date, DependencyPath>, DependencyPath> {

    @Override
    public DependencyPath map(Tuple2<Date, DependencyPath> value) throws Exception {
      value.f1.setTimeBucket(value.f0);
      return value.f1;
    }
  }
}
