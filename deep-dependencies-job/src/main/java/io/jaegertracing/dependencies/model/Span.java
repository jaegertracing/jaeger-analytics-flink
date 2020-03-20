package io.jaegertracing.dependencies.model;

import java.io.Serializable;
import lombok.Data;

@Data
public class Span implements Serializable {

  private static final long serialVersionUID = 0L;
  private long traceIdLow;
  private long spanId;
  private long parentSpanId;
  private String serviceName;
  private String operationName;
  private Kind spanKind;

  public enum Kind {
    SERVER,
    CLIENT,
    LOCAL,
    UNKNOWN
  }
}
