package io.jaegertracing.tracequality;

import com.uber.jaeger.Process;
import com.uber.jaeger.SpanRef;
import com.uber.jaeger.Tag;
import io.jaegertracing.tracequality.model.Span;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.util.List;

/**
 * Converts an {@link com.uber.jaeger.Span} into a {@link Span} by discarding unnecessary fields.
 */
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
                    if ("child_of".equalsIgnoreCase(ref.getRefType())) {
                        span.setParentSpanId(ref.getSpanId());
                    }
                }
            }
        }

        List<Tag> tags = jSpan.getTags();
        if (tags != null) {
            for (Tag tag : tags) {
                String key = tag.getKey();
                String val = tag.getVStr();
                if ("client".equalsIgnoreCase(val) && "span.kind".equalsIgnoreCase(key)) {
                    span.setClient(true);
                }
                if ("server".equalsIgnoreCase(val) && "span.kind".equalsIgnoreCase(key)) {
                    span.setServer(true);
                }
                if ("peer.service".equalsIgnoreCase(key) && val != null) {
                    span.setPeerService(val.toLowerCase());
                }
            }
        }

        Process process = jSpan.getProcess();
        span.setServiceName(process.getServiceName());

        List<Tag> processTags = process.getTags();
        if (processTags != null) {
            for (Tag tag : processTags) {
                if ("jaeger.version".equalsIgnoreCase(tag.getKey()) && tag.getVStr() != null) {
                    span.setClientVersion(tag.getVStr());
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

