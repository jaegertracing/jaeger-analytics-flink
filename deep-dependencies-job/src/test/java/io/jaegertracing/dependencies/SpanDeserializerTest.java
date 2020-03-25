package io.jaegertracing.dependencies;

import com.uber.jaeger.Process;
import com.uber.jaeger.SpanRef;
import com.uber.jaeger.Tag;
import io.jaegertracing.dependencies.model.Span;
import java.util.Collections;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class SpanDeserializerTest {

  private final long traceIdLow = 1235;
  private final long spanId = 4444;
  private final String serviceName = "boop";
  private final String operationName = "getDoggo";

  private SpanDeserializer spanDeserializer = new SpanDeserializer();

  private com.uber.jaeger.Span createSpan() {
    com.uber.jaeger.Span jSpan = new com.uber.jaeger.Span();
    jSpan.setTraceIdLow(traceIdLow);
    jSpan.setSpanId(spanId);
    jSpan.setOperationName(operationName);

    Process process = new com.uber.jaeger.Process();
    process.setServiceName(serviceName);
    jSpan.setProcess(process);

    return jSpan;
  }

  private com.uber.jaeger.Span createSpanWithKind(String spanKind) {
    com.uber.jaeger.Span jSpan = createSpan();

    Tag tag = new Tag();
    tag.setVStr(spanKind);
    tag.setKey("span.kind");
    jSpan.setTags(Collections.singletonList(tag));
    return jSpan;
  }

  @Test
  public void mapRequiredFields() throws Exception {
    com.uber.jaeger.Span jSpan = createSpan();

    Span span = spanDeserializer.map(jSpan);
    Assertions.assertThat(span.getTraceIdLow()).isEqualTo(traceIdLow);
    Assertions.assertThat(span.getSpanId()).isEqualTo(spanId);
    Assertions.assertThat(span.getServiceName()).isEqualTo(serviceName);
  }

  @Test
  public void mapServerTag() throws Exception {
    com.uber.jaeger.Span jSpan = createSpanWithKind("server");

    Span span = spanDeserializer.map(jSpan);
    Assertions.assertThat(span.getSpanKind()).isEqualTo(Span.Kind.SERVER);
  }

  @Test
  public void mapClientTag() throws Exception {
    com.uber.jaeger.Span jSpan = createSpanWithKind("client");

    Span span = spanDeserializer.map(jSpan);
    Assertions.assertThat(span.getSpanKind()).isEqualTo(Span.Kind.CLIENT);
  }

  @Test
  public void mapLocalTag() throws Exception {
    com.uber.jaeger.Span jSpan = createSpanWithKind("local");

    Span span = spanDeserializer.map(jSpan);
    Assertions.assertThat(span.getSpanKind()).isEqualTo(Span.Kind.LOCAL);
  }

  @Test
  public void mapUnknownTag() throws Exception {
    com.uber.jaeger.Span jSpan = createSpanWithKind("arbitrary");

    Span span = spanDeserializer.map(jSpan);
    Assertions.assertThat(span.getSpanKind()).isEqualTo(Span.Kind.UNKNOWN);
  }

  @Test
  public void mapParentSpan() throws Exception {
    long parentSpanId = 123;

    com.uber.jaeger.Span jSpan = createSpan();
    jSpan.setParentSpanId(parentSpanId);

    Assertions.assertThat(spanDeserializer.map(jSpan).getParentSpanId()).isEqualTo(parentSpanId);
  }

  @Test
  public void mapParentSpanFromReference() throws Exception {
    long parentSpanId = 123;

    SpanRef spanRef = new SpanRef();
    spanRef.setRefType("child_of");
    spanRef.setSpanId(parentSpanId);

    com.uber.jaeger.Span jSpan = createSpan();
    jSpan.setReferences(Collections.singletonList(spanRef));

    Assertions.assertThat(spanDeserializer.map(jSpan).getParentSpanId()).isEqualTo(parentSpanId);
  }

  @Test
  public void mapOperationName() throws Exception {
    com.uber.jaeger.Span jSpan = createSpan();
    Assertions.assertThat(spanDeserializer.map(jSpan).getOperationName()).isEqualTo(operationName);
  }
}
