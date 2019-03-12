package io.jaegertracing.tracequality;

import io.jaegertracing.analytics.JaegerJob;
import com.uber.jaeger.Span;
import io.jaegertracing.tracequality.model.QualityScore;
import io.jaegertracing.tracequality.score.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

/**
 * Computes Trace Quality scores for Traces.
 */
public class TraceQualityJob implements JaegerJob<QualityScore> {

    private static final Time traceCompletionTime = Time.minutes(3);
    private static final Time cassandraAggregationTime = Time.hours(1);

    public static void main(String[] args) throws Exception {
        TraceQualityJob job = new TraceQualityJob();
        job.executeJob("Trace quality Job", TypeInformation.of(QualityScore.class));
    }

    @Override
    public void setupJob(ParameterTool parameterTool, DataStream<Span> spans, SinkFunction<QualityScore> sinkFunction) throws Exception {

        SingleOutputStreamOperator<QualityScore> qualityMetricStream =
                spans.map(new SpanDeserializer()).name("DeserializeSpan")
                        .filter(value -> (value.isClient() || value.isServer())).name("FilterLocalSpans")
                        .keyBy(io.jaegertracing.tracequality.model.Span::getTraceIdLow)
                        // TODO: Add support for 128bit traceIds
                        .window(EventTimeSessionWindows.withGap(traceCompletionTime))
                        .apply(new TraceToQualityMetrics()).name("ComputeQualityMetrics");


        SingleOutputStreamOperator<QualityScore> qualityMetricCounts =
                qualityMetricStream
                        .keyBy(v -> String.format("%d:%s:%s:%s", v.getTimeBucket().getTime(), v.getService(), v.getMetric(), v.getSubmetric()))
                        .timeWindow(cassandraAggregationTime)
                        .sum("count")
                        .name("AggregateQualityMetricsCountsAcrossTraces");

        qualityMetricCounts.addSink(sinkFunction).name("CassandraScoreSink");
    }

    private static class TraceToQualityMetrics extends RichWindowFunction<io.jaegertracing.tracequality.model.Span, QualityScore, Long, TimeWindow> {
        private transient QualityScoreProcessor qualityScoreProcessor;

        @Override
        public void open(Configuration parameters) throws Exception {
            qualityScoreProcessor = new QualityScoreProcessor();
            qualityScoreProcessor.addSpanBasedQualityScore(new ClientVersion());
            qualityScoreProcessor.addSpanBasedQualityScore(new MinimumClientVersion());

            qualityScoreProcessor.addTraceBasedQualityScore(new HasClientServerSpans());
            qualityScoreProcessor.addTraceBasedQualityScore(new UniqueSpanId());
        }

        @Override
        public void apply(Long traceId, TimeWindow window, Iterable<io.jaegertracing.tracequality.model.Span> input, Collector<QualityScore> out) throws Exception {
            Date dateBucket = Date.from(Instant.ofEpochMilli(window.getStart()).truncatedTo(ChronoUnit.HOURS));
            for (QualityScore metricCount : qualityScoreProcessor.compute(input, dateBucket)) {
                metricCount.setTraceId(Long.toHexString(traceId)); // TODO: set this inside QualityScoreAggregator
                out.collect(metricCount);
            }
        }
    }
}

