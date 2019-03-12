package io.jaegertracing.dependencies;

import com.uber.jaeger.Process;
import com.uber.jaeger.SpanRef;
import com.uber.jaeger.Tag;
import io.jaegertracing.dependencies.model.Span;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.util.List;

class SpanDeserializer extends RichMapFunction<com.uber.jaeger.Span, Span> implements ResultTypeQueryable<Span> {

    @Override
    public Span map(com.uber.jaeger.Span jSpan) throws Exception {
        Span span = new Span();
        span.setTraceIdLow(jSpan.getTraceIdLow());
        span.setTraceIdHigh(jSpan.getTraceIdHigh());
        span.setSpanId(jSpan.getSpanId());

        if (jSpan.getParentSpanId() != null) {
            span.setParentSpanId(jSpan.getParentSpanId());
        } else {
            List<SpanRef> refs = jSpan.getReferences();
            if (refs != null) {
                for (SpanRef ref : refs) {
                    if ("child_of".equals(ref.getRefType())) {
                        span.setParentSpanId(ref.getSpanId());
                    }
                }
            }
        }

        Process process = jSpan.getProcess();
        span.setServiceName(process.getServiceName());

        List<Tag> tags = jSpan.getTags();
        if (tags != null) {
            for (Tag tag : tags) {
                String str = tag.getVStr();
                if ("client".equals(str) && "span.kind".equals(tag.getKey())) {
                    span.setClient(true);
                }
                if ("server".equals(str) && "span.kind".equals(tag.getKey())) {
                    span.setServer(true);
                }
            }
        }

        return span;
    }

    @Override
    public TypeInformation<Span> getProducedType() {
        return TypeInformation.of(Span.class);
    }
}
