package com.example.burnrate;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

/**
 * Phase 0: read raw messages from Kafka/Redpanda and print them. No parsing yet.
 */
public class BurnRateJob {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9093");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "cloudtrail-events");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("burn-rate-job")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawEvents = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "cloudtrail-events-source");

        rawEvents.print();

        env.execute("flink-cloud-burn-rate");
    }
}
