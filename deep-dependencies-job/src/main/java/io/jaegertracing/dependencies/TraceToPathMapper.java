package io.jaegertracing.dependencies;

import io.jaegertracing.dependencies.model.Span;
import io.jaegertracing.depenencies.model.ServiceOperation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class TraceToPathMapper {

  private Map<Long, List<Long>> spanIdToChildren = new HashMap<>();
  private Map<Long, Span> spanIdToSpan = new HashMap<>();
  private Set<Long> visited = new HashSet<>();

  private List<List<ServiceOperation>> paths = new ArrayList<>();

  private TraceToPathMapper(Iterable<Span> spans) {
    Span rootSpan = null;

    for (Span span : spans) {
      spanIdToSpan.put(span.getSpanId(), span);
      if (span.getParentSpanId() == 0) {
        rootSpan = span;
      } else {
        spanIdToChildren
            .computeIfAbsent(span.getParentSpanId(), y -> new ArrayList<>())
            .add(span.getSpanId());
      }
    }

    if (rootSpan != null) {
      process(rootSpan.getSpanId(), new ArrayList<>());
    }
  }

  /**
   * Computes all paths in the trace starting from the root span. If the root span is missing, no
   * paths are computed.
   *
   * <p>Example:
   *
   * <p>Input: A, B, C, D with relationship A->B, B->C, B->D, A->B means B is child of A Output:
   * {{A,B,C}, {A,B,D}}
   *
   * @param spans belonging to the same trace
   */
  static List<List<ServiceOperation>> getPaths(Iterable<Span> spans) {
    TraceToPathMapper mapper = new TraceToPathMapper(spans);
    return mapper.paths;
  }

  private void process(Long spanId, List<Long> path) {
    visited.add(spanId);
    path.add(spanId);

    List<Long> childrenSpanIds = spanIdToChildren.get(spanId);
    if (childrenSpanIds != null) {
      for (Long childSpanId : childrenSpanIds) {
        if (!visited.contains(childSpanId)) {
          process(childSpanId, new ArrayList<>(path));
        }
      }
    } else {
      mapSpanIdsToServiceOperations(path);
    }
  }

  /**
   * This method can produce incorrect output when there are missing server spans in the trace path
   * TODO: Use the service name to detect such cases to make this more robust
   */
  private void mapSpanIdsToServiceOperations(List<Long> spanIdPath) {
    List<ServiceOperation> result = new ArrayList<>(spanIdPath.size());
    for (Long spanId : spanIdPath) {
      Span span = spanIdToSpan.get(spanId);

      if (span.getSpanKind() == Span.Kind.SERVER) {
        result.add(new ServiceOperation(span.getServiceName(), span.getOperationName()));
      }
    }
    paths.add(result);
  }
}
