package io.jaegertracing.analytics.adjuster;


public interface Dedupable {
    long getSpanId();

    void setSpanId(long spanId);

    long getParentSpanId();

    void setParentSpanId(long parentSpanId);

    boolean isClient();

    boolean isServer();
}
