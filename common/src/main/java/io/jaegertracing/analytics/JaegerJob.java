package io.jaegertracing.analytics;

import com.uber.jaeger.Span;
import io.jaegertracing.analytics.avro.SpanDeserializer;
import io.jaegertracing.analytics.cassandra.ClusterBuilder;
import io.jaegertracing.analytics.config.Utils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraPojoSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public interface JaegerJob<T> {
    /**
     * These constants are used to provide user friendly names for Flink operators. Flink also uses them in
     * metric names.
     */
    String KAFKA_SOURCE = "KafkaSource";

    Logger logger = LoggerFactory.getLogger(JaegerJob.class);

    void setupJob(ParameterTool parameterTool, DataStream<Span> spans, SinkFunction<T> sinkFunction) throws Exception;

    default void executeJob(String jobName, TypeInformation<T> sinkType) throws Exception {
        logger.info("Beginning job execution");

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(System.getProperty("CONFIG_PATH"));

        FlinkKafkaConsumer<Span> consumer = configureKafkaConsumer(parameterTool);
        SinkFunction<T> sink = configureSink(parameterTool, sinkType);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnvironmentForJob(parameterTool, env);

        SingleOutputStreamOperator<Span> spanSource = env.addSource(consumer).name(KAFKA_SOURCE);
        setupJob(parameterTool, spanSource, sink);

        env.execute(jobName);
    }

    default FlinkKafkaConsumer<Span> configureKafkaConsumer(ParameterTool parameterTool) {
        logger.info("Setting up Kafka");
        String topic = parameterTool.get("kafka.topic", "");
        Properties kafkaProperties = Utils.filterPrefix(parameterTool.getProperties(), "kafka");
        FlinkKafkaConsumer<Span> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SpanDeserializer(), kafkaProperties);

        kafkaConsumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Span>(Time.minutes(10)) {
            @Override
            public long extractTimestamp(Span element) {
                return element.getStartTime() / 1000; // Convert microseconds to milliseconds
            }
        });
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
    }

    default SinkFunction<T> configureSink(ParameterTool parameterTool, TypeInformation<T> typeInformation) {
        logger.info("Setting up Cassandra");
        String contactPoint = parameterTool.get("cassandra.contactpoint", "localhost");
        String keySpace = parameterTool.get("cassandra.keyspace", "jaeger_v1_local");
        short port = parameterTool.getShort("cassandra.port", (short) 9042);
        String username = parameterTool.get("cassandra.username");
        String password = parameterTool.get("cassandra.password");

        ClusterBuilder clusterBuilder = ClusterBuilder.builder()
                .contactPoints(contactPoint)
                .port(port)
                .username(username)
                .password(password)
                .build();

        return new CassandraPojoSink<T>(typeInformation.getTypeClass(), clusterBuilder, keySpace);
    }

    default void configureEnvironmentForJob(ParameterTool parameterTool, StreamExecutionEnvironment env) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ExecutionConfig executionConfig = env.getConfig();
        executionConfig.setMaxParallelism(Runtime.getRuntime().availableProcessors());
        executionConfig.enableObjectReuse();
        executionConfig.setGlobalJobParameters(parameterTool);

        // TODO: Read this from parameterTool
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        checkpointConfig.setCheckpointInterval(Time.minutes(10).toMilliseconds());
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setMinPauseBetweenCheckpoints(Time.minutes(5).toMilliseconds());
    }
}
