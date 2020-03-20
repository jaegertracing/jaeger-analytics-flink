package io.jaegertracing.dependencies;

import io.jaegertracing.dependencies.model.Span;
import io.jaegertracing.depenencies.model.ServiceOperation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TraceToPathMapperTest {

  private static final String SERVICE_A = "Ada";
  private static final String SERVICE_B = "Bonda";
  private static final String SERVICE_C = "MasalWada";

  private static final String OPERATION_A = "fry";
  private static final String OPERATION_B = "bake";
  private static final String OPERATION_C = "poach";

  private static final String TRACE_ID = "909";

  private Span makeSpan(
      long spanId,
      long parentSpanId,
      String serviceName,
      String operationName,
      Span.Kind spanKind) {
    Span span = new Span();
    span.setSpanId(spanId);
    span.setParentSpanId(parentSpanId);
    span.setServiceName(serviceName);
    span.setOperationName(operationName);
    span.setSpanKind(spanKind);
    return span;
  }

  @Test
  public void TestRootNodeOnly() throws Exception {
    List<Span> spans = new ArrayList<>();
    spans.add(makeSpan(1, 0, SERVICE_A, OPERATION_A, Span.Kind.SERVER));

    Assertions.assertThat(TraceToPathMapper.getPaths(spans))
        .containsOnly(Collections.singletonList(new ServiceOperation(SERVICE_A, OPERATION_A)));
  }

  @Test
  public void TestIgnoreNonServerSpans() throws Exception {
    List<Span> spans = new ArrayList<>();
    spans.add(makeSpan(1, 0, SERVICE_A, OPERATION_A, Span.Kind.SERVER));
    spans.add(makeSpan(2, 1, SERVICE_A, OPERATION_B, Span.Kind.CLIENT));
    spans.add(makeSpan(3, 1, SERVICE_A, OPERATION_C, Span.Kind.LOCAL));
    spans.add(makeSpan(4, 1, SERVICE_A, OPERATION_B, Span.Kind.UNKNOWN));

    Assertions.assertThat(TraceToPathMapper.getPaths(spans))
        .containsOnly(Collections.singletonList((new ServiceOperation(SERVICE_A, OPERATION_A))));
  }

  @Test
  public void TestTreeWithTwoChildren() throws Exception {
    List<Span> spans = new ArrayList<>();
    spans.add(makeSpan(1, 0, SERVICE_A, OPERATION_A, Span.Kind.SERVER));
    spans.add(makeSpan(2, 1, SERVICE_B, OPERATION_C, Span.Kind.SERVER));
    spans.add(makeSpan(3, 1, SERVICE_C, OPERATION_B, Span.Kind.SERVER));

    List<ServiceOperation> expectedPath1 = new ArrayList<>();
    expectedPath1.add(new ServiceOperation(SERVICE_A, OPERATION_A));
    expectedPath1.add(new ServiceOperation(SERVICE_B, OPERATION_C));

    List<ServiceOperation> expectedPath2 = new ArrayList<>();
    expectedPath2.add(new ServiceOperation(SERVICE_A, OPERATION_A));
    expectedPath2.add(new ServiceOperation(SERVICE_C, OPERATION_B));

    Assertions.assertThat(TraceToPathMapper.getPaths(spans)).contains(expectedPath1, expectedPath2);
  }

  @Test
  public void TestCircularDependencies() throws Exception {
    List<Span> spans = new ArrayList<>();
    spans.add(makeSpan(1, 0, SERVICE_A, OPERATION_A, Span.Kind.SERVER));
    spans.add(makeSpan(0, 1, SERVICE_B, OPERATION_C, Span.Kind.SERVER));

    List<ServiceOperation> path = new ArrayList<>();
    path.add(new ServiceOperation(SERVICE_A, OPERATION_A));
    path.add(new ServiceOperation(SERVICE_B, OPERATION_C));

    Assertions.assertThat(TraceToPathMapper.getPaths(spans)).containsOnly(path);
  }

  @Test
  public void TestNonRootNode() throws Exception {
    List<Span> spans = new ArrayList<>();
    spans.add(makeSpan(0, 1, SERVICE_B, OPERATION_C, Span.Kind.SERVER));

    Assertions.assertThat(TraceToPathMapper.getPaths(spans)).isEmpty();
  }

  @Test
  public void TestDuplicatedPath() throws Exception {
    List<Span> spans = new ArrayList<>();
    spans.add(makeSpan(1, 0, SERVICE_A, OPERATION_A, Span.Kind.SERVER));
    spans.add(makeSpan(2, 1, SERVICE_A, OPERATION_A, Span.Kind.CLIENT));

    spans.add(makeSpan(4, 2, SERVICE_B, OPERATION_C, Span.Kind.SERVER));
    spans.add(makeSpan(5, 4, SERVICE_B, OPERATION_C, Span.Kind.CLIENT));

    spans.add(makeSpan(6, 5, SERVICE_C, OPERATION_A, Span.Kind.SERVER));
    spans.add(makeSpan(7, 5, SERVICE_C, OPERATION_A, Span.Kind.SERVER));

    spans.add(makeSpan(8, 5, SERVICE_C, OPERATION_B, Span.Kind.SERVER));

    List<ServiceOperation> expectedPath1 = new ArrayList<>();
    expectedPath1.add(new ServiceOperation(SERVICE_A, OPERATION_A));
    expectedPath1.add(new ServiceOperation(SERVICE_B, OPERATION_C));
    expectedPath1.add(new ServiceOperation(SERVICE_C, OPERATION_A));

    List<ServiceOperation> expectedPath2 = new ArrayList<>();
    expectedPath2.add(new ServiceOperation(SERVICE_A, OPERATION_A));
    expectedPath2.add(new ServiceOperation(SERVICE_B, OPERATION_C));
    expectedPath2.add(new ServiceOperation(SERVICE_C, OPERATION_B));

    Assertions.assertThat(TraceToPathMapper.getPaths(spans))
        .contains(expectedPath1, expectedPath1, expectedPath2);
  }
}
