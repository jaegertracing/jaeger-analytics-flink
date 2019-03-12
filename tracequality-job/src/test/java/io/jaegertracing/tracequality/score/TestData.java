package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.Span;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestData {
    public static final String PASS = "PASS";
    public static final String FAIL = "FAIL";

    private final Map<Long, List<Span>> spanIdToSpans = new HashMap<>();

    enum SpanKind {
        CLIENT, SERVER
    }

    public void addSpan(long spanId, long parentSpanId, SpanKind kind, String serviceName, String peerService) {
        addSpan(newSpan(spanId, parentSpanId, kind, serviceName, peerService));
    }

    public void addSpan(Span span) {
        spanIdToSpans.computeIfAbsent(span.getSpanId(), l -> new ArrayList<>()).add(span);
    }

    public Map<Long, List<Span>> getSpanIdToSpans() {
        return spanIdToSpans;
    }

    public Span newSpan(long spanId, long parentSpanId, SpanKind kind, String serviceName, String peerService) {
        Span span = new Span();
        span.setSpanId(spanId);
        span.setParentSpanId(parentSpanId);
        if (SpanKind.CLIENT.equals(kind)) {
            span.setClient(true);
        }
        if (SpanKind.SERVER.equals(kind)) {
            span.setServer(true);
        }
        span.setServiceName(serviceName);
        span.setPeerService(peerService);
        return span;
    }
}
