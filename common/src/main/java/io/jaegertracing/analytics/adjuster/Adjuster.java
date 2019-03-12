package io.jaegertracing.analytics.adjuster;

import java.io.Serializable;

public interface Adjuster<T> extends Serializable {
    Iterable<T> adjust(Iterable<T> trace);
}
