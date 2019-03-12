package io.jaegertracing.tracequality.model;

import io.jaegertracing.analytics.adjuster.Dedupable;
import lombok.*;

import java.io.Serializable;

@Data
public class Span implements Serializable, Dedupable {
    private static final long serialVersionUID = 0L;
    private long traceIdLow;
    private long traceIdHigh;
    private long spanId;
    private long parentSpanId;

    private String serviceName;
    private String peerService;

    private boolean server;
    private boolean client;

    private String clientVersion;
}

