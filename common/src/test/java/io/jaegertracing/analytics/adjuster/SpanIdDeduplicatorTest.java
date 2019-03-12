package io.jaegertracing.analytics.adjuster;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SpanIdDeduplicatorTest {

    @Test
    public void TestAdjusterTriggered() {
        long clientSpanIdOne = 100;
        long clientSpanIdTwo = 201;
        long otherChildSpanId = 11;

        List<TestSpan> trace = new ArrayList<>();
        trace.add(new TestSpan(clientSpanIdOne, 0, false, true));
        trace.add(new TestSpan(clientSpanIdOne, 0, true, false));
        trace.add(new TestSpan(otherChildSpanId, clientSpanIdOne, true, false));
        trace.add(new TestSpan(clientSpanIdTwo, otherChildSpanId, false, true));
        trace.add(new TestSpan(clientSpanIdTwo, otherChildSpanId, true, false));

        SpanIdDeduplicator<TestSpan> deduplicator = new SpanIdDeduplicator<>();
        Iterable<TestSpan> adjustedTrace = deduplicator.adjust(trace);

        Iterator<TestSpan> iterator = adjustedTrace.iterator();
        TestSpan clientSpan = iterator.next();
        assertThat(clientSpanIdOne).isEqualTo(clientSpan.getSpanId());

        TestSpan serverSpan = iterator.next();
        assertThat(serverSpan.getSpanId()).isEqualTo(1);
        assertThat(clientSpan.getSpanId()).isEqualTo(serverSpan.getParentSpanId());

        TestSpan otherChild = iterator.next();
        assertThat(otherChild.getSpanId()).isEqualTo(otherChildSpanId);
        assertThat(serverSpan.getSpanId()).isEqualTo(otherChild.getParentSpanId());

        TestSpan secondClientSpan = iterator.next();
        TestSpan secondServerSpan = iterator.next();
        assertThat(secondServerSpan.getSpanId()).isEqualTo(2);
        assertThat(secondClientSpan.getSpanId()).isEqualTo(secondServerSpan.getParentSpanId());


    }

    @Test
    public void testAdjusterNotTriggered() {
        long serverSpanId = 100;
        long anotherSpanId = 11;
        List<TestSpan> trace = new ArrayList<>();
        trace.add(new TestSpan(serverSpanId, 0, true, false));
        trace.add(new TestSpan(anotherSpanId, serverSpanId, false, false));

        SpanIdDeduplicator<TestSpan> deduplicator = new SpanIdDeduplicator<>();
        Iterable<TestSpan> adjustedTrace = deduplicator.adjust(trace);

        Iterator<TestSpan> iterator = adjustedTrace.iterator();
        TestSpan serverSpan = iterator.next();
        assertThat(serverSpan.getSpanId()).isEqualTo(serverSpanId);

        TestSpan otherChild = iterator.next();
        assertThat(otherChild.getSpanId()).isEqualTo(anotherSpanId);
        assertThat(serverSpan.getSpanId()).isEqualTo(otherChild.getParentSpanId());
    }

    @Test(expected = IllegalStateException.class)
    public void testMaxSpanId() {
        SpanIdDeduplicator<TestSpan> deduplicator = new SpanIdDeduplicator<>();
        deduplicator.getNextUnusedSpanId(new HashMap<>(), Long.MAX_VALUE - 1);
    }


}