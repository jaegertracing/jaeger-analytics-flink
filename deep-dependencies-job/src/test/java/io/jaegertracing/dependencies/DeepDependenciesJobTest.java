package io.jaegertracing.dependencies;

import com.google.common.collect.ImmutableList;
import com.uber.jaeger.Process;
import com.uber.jaeger.Span;
import com.uber.jaeger.Tag;
import io.jaegertracing.depenencies.model.DependencyPath;
import io.jaegertracing.depenencies.model.ServiceOperation;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.assertj.core.api.Assertions;
import org.assertj.core.groups.Tuple;
import org.junit.Test;

public class DeepDependenciesJobTest {

  private final ServiceOperation ServiceOperation_A =
      new ServiceOperation("service_A", "operation_A");
  private final ServiceOperation ServiceOperation_B =
      new ServiceOperation("service_B", "operation_B");
  private final ServiceOperation ServiceOperation_C =
      new ServiceOperation("service_C", "operation_C");
  private final ServiceOperation Service_A =
      new ServiceOperation("service_A", ServiceOperation.ALL_OPERATIONS);
  private final ServiceOperation Service_B =
      new ServiceOperation("service_B", ServiceOperation.ALL_OPERATIONS);
  private final ServiceOperation Service_C =
      new ServiceOperation("service_C", ServiceOperation.ALL_OPERATIONS);

  private final Instant testTime = Instant.parse("2007-12-03T10:30:12Z");
  private final Instant testTimeTruncatedToHour = Instant.parse("2007-12-03T10:00:00Z");
  private final Long traceId = 100L;

  private Span makeSpan(long spanId, long parentSpanId, String serviceName, String operation) {
    Span span = new Span();
    span.setTraceIdLow(traceId);
    span.setSpanId(spanId);
    span.setParentSpanId(parentSpanId);
    span.setOperationName(operation);
    span.setStartTime(testTime.getEpochSecond() * 1000);

    Tag tag = new Tag();
    tag.setKey("span.kind");
    tag.setVStr("server");
    tag.setVType("vStr");

    span.setTags(Collections.singletonList(tag));

    Process process = new Process();
    process.setServiceName(serviceName);

    span.setProcess(process);
    return span;
  }

  @Test
  public void testDeepDependenciesJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setParallelism(4);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    DataStreamSource<Span> spans =
        env.fromElements(
            makeSpan(1, 0, ServiceOperation_A.getService(), ServiceOperation_A.getOperation()),
            makeSpan(2, 1, ServiceOperation_B.getService(), ServiceOperation_B.getOperation()),
            makeSpan(3, 1, ServiceOperation_C.getService(), ServiceOperation_C.getOperation()));

    SingleOutputStreamOperator<Span> spansWithTime =
        spans.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<Span>(Time.seconds(0)) {
              @Override
              public long extractTimestamp(Span element) {
                return element.getStartTime();
              }
            });

    DeepDependenciesJob deepDependenciesJob = new DeepDependenciesJob();
    ParameterTool params = ParameterTool.fromMap(Collections.emptyMap());
    deepDependenciesJob.setupJob(params, spansWithTime, new CollectSink());

    env.execute();

    Assertions.assertThat(CollectSink.values).hasSize(8);
    Assertions.assertThat(CollectSink.values)
        .extracting(DependencyPath::getTimeBucket)
        .containsOnly(Date.from(testTimeTruncatedToHour));
    Assertions.assertThat(CollectSink.values)
        .extracting(DependencyPath::getFocalNode, DependencyPath::getPath)
        .contains(
            Tuple.tuple(
                ServiceOperation_A, ImmutableList.of(ServiceOperation_A, ServiceOperation_B)),
            Tuple.tuple(
                ServiceOperation_A, ImmutableList.of(ServiceOperation_A, ServiceOperation_C)),
            Tuple.tuple(
                ServiceOperation_B, ImmutableList.of(ServiceOperation_A, ServiceOperation_B)),
            Tuple.tuple(
                ServiceOperation_C, ImmutableList.of(ServiceOperation_A, ServiceOperation_C)),
            Tuple.tuple(Service_A, ImmutableList.of(ServiceOperation_A, ServiceOperation_B)),
            Tuple.tuple(Service_A, ImmutableList.of(ServiceOperation_A, ServiceOperation_C)),
            Tuple.tuple(Service_B, ImmutableList.of(ServiceOperation_A, ServiceOperation_B)),
            Tuple.tuple(Service_C, ImmutableList.of(ServiceOperation_A, ServiceOperation_C)));
  }

  private static class CollectSink implements SinkFunction<DependencyPath> {

    public static final List<DependencyPath> values = new ArrayList<>();

    @Override
    public synchronized void invoke(DependencyPath path, Context context) throws Exception {
      values.add(path);
    }
  }
}
