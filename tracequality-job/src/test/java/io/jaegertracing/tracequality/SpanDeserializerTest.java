package io.jaegertracing.tracequality;

import com.uber.jaeger.Process;
import com.uber.jaeger.SpanRef;
import com.uber.jaeger.Tag;
import io.jaegertracing.tracequality.model.Span;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Collections;

public class SpanDeserializerTest {
    private final long traceIdLow = 1235;
    private final long traceIdHigh = 3333;
    private final long spanId = 4444;
    private final String serviceName = "boop";

    private final SpanDeserializer spanDeserializer = new SpanDeserializer();

    private com.uber.jaeger.Span getSpan() {
        com.uber.jaeger.Span jSpan = new com.uber.jaeger.Span();
        jSpan.setTraceIdLow(traceIdLow);
        jSpan.setTraceIdHigh(traceIdHigh);
        jSpan.setSpanId(spanId);

        Process process = new com.uber.jaeger.Process();
        process.setServiceName(serviceName);
        jSpan.setProcess(process);

        return jSpan;
    }

    @Test
    public void mapRequiredFields() throws Exception {
        com.uber.jaeger.Span jSpan = getSpan();

        Span span = spanDeserializer.map(jSpan);
        Assertions.assertThat(span.getTraceIdLow()).isEqualTo(traceIdLow);
        Assertions.assertThat(span.getTraceIdHigh()).isEqualTo(traceIdHigh);
        Assertions.assertThat(span.getSpanId()).isEqualTo(spanId);
        Assertions.assertThat(span.getServiceName()).isEqualTo(serviceName);
    }

    @Test
    public void mapServerTag() throws Exception {
        com.uber.jaeger.Span jSpan = getSpan();

        Tag tag = new Tag();
        tag.setVStr("server");
        tag.setKey("span.kind");
        jSpan.setTags(Collections.singletonList(tag));

        Span span = spanDeserializer.map(jSpan);
        Assertions.assertThat(span.isServer()).isTrue();
        Assertions.assertThat(span.isClient()).isFalse();
    }

    @Test
    public void mapClientTag() throws Exception {
        com.uber.jaeger.Span jSpan = getSpan();

        Tag tag = new Tag();
        tag.setVStr("client");
        tag.setKey("span.kind");
        jSpan.setTags(Collections.singletonList(tag));

        Span span = spanDeserializer.map(jSpan);
        Assertions.assertThat(span.isServer()).isFalse();
        Assertions.assertThat(span.isClient()).isTrue();
    }

    @Test
    public void mapPeerServiceTag() throws Exception {
        com.uber.jaeger.Span jSpan = getSpan();

        Tag tag = new Tag();
        tag.setKey("peer.service");
        tag.setVStr("rtapi");
        jSpan.setTags(Collections.singletonList(tag));

        Span span = spanDeserializer.map(jSpan);
        Assertions.assertThat(span.getPeerService()).isEqualTo("rtapi");
    }

    @Test
    public void mapClientVersion() throws Exception {
        com.uber.jaeger.Span jSpan = getSpan();

        Tag tag = new Tag();
        tag.setKey("jaeger.version");
        tag.setVStr("go-1.2.3");

        Process process = new com.uber.jaeger.Process();
        process.setTags(Collections.singletonList(tag));
        jSpan.setProcess(process);

        Span span = spanDeserializer.map(jSpan);
        Assertions.assertThat(span.getClientVersion()).isEqualTo("go-1.2.3");
    }

    @Test
    public void mapParentSpan() throws Exception {
        long parentSpanId = 123;

        com.uber.jaeger.Span jSpan = getSpan();
        jSpan.setParentSpanId(parentSpanId);


        Assertions.assertThat(spanDeserializer.map(jSpan).getParentSpanId()).isEqualTo(parentSpanId);
    }

    @Test
    public void mapParentSpanFromReference() throws Exception {
        long parentSpanId = 123;

        SpanRef spanRef = new SpanRef();
        spanRef.setRefType("child_of");
        spanRef.setSpanId(parentSpanId);

        com.uber.jaeger.Span jSpan = getSpan();
        jSpan.setReferences(Collections.singletonList(spanRef));

        Assertions.assertThat(spanDeserializer.map(jSpan).getParentSpanId()).isEqualTo(parentSpanId);
    }


    @Test
    public void getProducedType() {
        SpanDeserializer spanDeserializer = new SpanDeserializer();
        Assertions.assertThat(spanDeserializer.getProducedType()).isEqualTo(TypeInformation.of(Span.class));
    }
}

