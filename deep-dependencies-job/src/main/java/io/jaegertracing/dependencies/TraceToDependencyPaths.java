package io.jaegertracing.dependencies;

import io.jaegertracing.dependencies.model.Span;
import io.jaegertracing.depenencies.model.DependencyPath;
import io.jaegertracing.depenencies.model.KeyValue;
import io.jaegertracing.depenencies.model.ServiceOperation;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

@Slf4j
public class TraceToDependencyPaths
    extends RichFlatMapFunction<Tuple2<String, Iterable<Span>>, DependencyPath> {

  @Override
  public void flatMap(Tuple2<String, Iterable<Span>> traceIdToSpans, Collector<DependencyPath> out)
      throws Exception {
    List<List<ServiceOperation>> paths = TraceToPathMapper.getPaths(traceIdToSpans.f1);

    if (paths.isEmpty()) {
      return;
    }

    for (List<ServiceOperation> serviceOperations : paths) {
      DependencyPath path = new DependencyPath();
      path.setPath(serviceOperations);
      path.setAttributes(
          Collections.singletonList(new KeyValue("exemplar_trace_id", traceIdToSpans.f0)));
      out.collect(path);
    }
  }
}
