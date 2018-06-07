/*
 * Copyright (c) 2018, The Jaeger Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.uber.jaeger.analytics.adjuster;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SpanIdDeduplicator<T extends Dedupable> implements Adjuster<T> {

  public Iterable<T> adjust(Iterable<T> trace) {
    Map<Long, Set<T>> spanIdToSpans = new HashMap<>();
    for (T span : trace) {
      spanIdToSpans.computeIfAbsent(span.getSpanId(), k -> new HashSet<>()).add(span);
    }

    dedupeSpanIds(trace, spanIdToSpans);
    return trace;
  }

  private void dedupeSpanIds(Iterable<T> trace, Map<Long, Set<T>> spanIdToSpans) {
    Map<Long, Long> oldSpanIdsToNew = new HashMap<>();
    long newSpanId = 0;
    for (T span : trace) {
      if (span.isServer() && spanIdToSpans.get(span.getSpanId()).stream().anyMatch(Dedupable::isClient)) {
        newSpanId = getNextUnusedSpanId(spanIdToSpans, newSpanId);
        oldSpanIdsToNew.put(span.getSpanId(), newSpanId);
        span.setParentSpanId(span.getSpanId());
        span.setSpanId(newSpanId);
      }
    }

    swapParentIds(trace, oldSpanIdsToNew);
  }

  private void swapParentIds(Iterable<T> trace, Map<Long, Long> oldSpanIdsToNew) {
    for (T span : trace) {
      Long newParentId = oldSpanIdsToNew.get(span.getParentSpanId());
      if (newParentId != null) {
        if (span.getSpanId() != newParentId) {
          span.setParentSpanId(newParentId);
        }
      }
    }
  }

  long getNextUnusedSpanId(Map<Long, Set<T>> spanIdToSpans, Long minId) {
    for (Long i = minId + 1; i < Long.MAX_VALUE; i++) {
      if (!spanIdToSpans.containsKey(minId)) {
        return i;
      }
    }
    throw new IllegalStateException("Cannot assign a free spanId, too many spans in trace");
  }

}
