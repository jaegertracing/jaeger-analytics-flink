package io.jaegertracing.dependencies;

import com.uber.jaeger.Process;
import com.uber.jaeger.SpanRef;
import com.uber.jaeger.Tag;
import io.jaegertracing.dependencies.model.Span;
import java.util.List;
import org.apache.flink.api.common.functions.RichMapFunction;

public class SpanDeserializer extends RichMapFunction<com.uber.jaeger.Span, Span> {

  @Override
  public Span map(com.uber.jaeger.Span jSpan) throws Exception {
    Span span = new Span();
    // TODO: Add 128-bit traceId support
    span.setTraceIdLow(jSpan.getTraceIdLow());
    span.setSpanId(jSpan.getSpanId());

    if (jSpan.getParentSpanId() != null) {
      span.setParentSpanId(jSpan.getParentSpanId());
    } else {
      List<SpanRef> refs = jSpan.getReferences();
      if (refs != null) {
        for (SpanRef ref : refs) {
          if ("child_of".equalsIgnoreCase(ref.getRefType())) {
            span.setParentSpanId(ref.getSpanId());
          }
        }
      }
    }

    span.setOperationName(jSpan.getOperationName());
    Process process = jSpan.getProcess();
    span.setServiceName(process.getServiceName());

    List<Tag> tags = jSpan.getTags();
    if (tags != null) {
      for (Tag tag : tags) {
        if ("span.kind".equals(tag.getKey())) {
          String tagValue = tag.getVStr().toLowerCase();
          switch (tagValue) {
            case "client":
              span.setSpanKind(Span.Kind.CLIENT);
              break;
            case "server":
              span.setSpanKind(Span.Kind.SERVER);
              break;
            case "local":
              span.setSpanKind(Span.Kind.LOCAL);
              break;
            default:
              span.setSpanKind(Span.Kind.UNKNOWN);
              break;
          }
        }
      }
    }
    return span;
  }
}
