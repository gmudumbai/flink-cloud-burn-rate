package com.example.burnrate;

import com.example.burnrate.model.BurnRateUpdate;
import com.example.burnrate.model.CloudTrailEvent;
import com.example.burnrate.model.InstanceLifecycleEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Phase 2: parse CloudTrail events, track per-instance lifecycle state keyed by
 * instanceId, and print the resulting BurnRateUpdate deltas.
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

        DataStream<InstanceLifecycleEvent> instanceEvents = rawEvents.flatMap(
                (String json, Collector<InstanceLifecycleEvent> out) -> {
                    for (InstanceLifecycleEvent e : CloudTrailEvent.parse(json)) {
                        out.collect(e);
                    }
                },
                org.apache.flink.api.common.typeinfo.TypeInformation.of(InstanceLifecycleEvent.class));

        SingleOutputStreamOperator<BurnRateUpdate> burnRateUpdates = instanceEvents
                .keyBy(e -> e.instanceId)
                .process(new ResourceLifecycleFunction(new PricingLookup()));

        burnRateUpdates.print();
        burnRateUpdates.getSideOutput(ResourceLifecycleFunction.UNMATCHED_TERMINATIONS)
                .map(e -> "UNMATCHED_TERMINATION " + e)
                .print();

        env.execute("flink-cloud-burn-rate");
    }
}
