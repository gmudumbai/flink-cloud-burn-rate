package com.example.burnrate;

import com.example.burnrate.model.BurnRateUpdate;
import com.example.burnrate.model.CloudTrailEvent;
import com.example.burnrate.model.InstanceLifecycleEvent;
import com.example.burnrate.model.RunningRateUpdate;
import com.example.burnrate.model.WindowDelta;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Phase 3: event-time watermarks, a 1-minute tumbling window per account summing
 * BurnRateUpdate deltas, and a running committed hourly rate. Late events (beyond
 * the allowed lateness) are routed to a side output instead of being dropped.
 */
public class BurnRateJob {

    private static final OutputTag<BurnRateUpdate> LATE_BURN_RATE_UPDATES =
            new OutputTag<BurnRateUpdate>("late-burn-rate-updates") {
            };

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9093");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "cloudtrail-events");
        long watermarkBoundSeconds = Long.parseLong(
                System.getenv().getOrDefault("WATERMARK_BOUND_SECONDS", "20"));
        long allowedLatenessSeconds = Long.parseLong(
                System.getenv().getOrDefault("ALLOWED_LATENESS_SECONDS", "30"));

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

        WatermarkStrategy<InstanceLifecycleEvent> instanceWatermarks = WatermarkStrategy
                .<InstanceLifecycleEvent>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkBoundSeconds))
                .withTimestampAssigner((event, recordTimestamp) -> event.eventTimeMillis)
                .withIdleness(Duration.ofSeconds(10));

        DataStream<InstanceLifecycleEvent> instanceEvents = rawEvents
                .flatMap(
                        (String json, Collector<InstanceLifecycleEvent> out) -> {
                            for (InstanceLifecycleEvent e : CloudTrailEvent.parse(json)) {
                                out.collect(e);
                            }
                        },
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(InstanceLifecycleEvent.class))
                .assignTimestampsAndWatermarks(instanceWatermarks);

        SingleOutputStreamOperator<BurnRateUpdate> burnRateUpdates = instanceEvents
                .keyBy(e -> e.instanceId)
                .process(new ResourceLifecycleFunction(new PricingLookup()));

        burnRateUpdates.getSideOutput(ResourceLifecycleFunction.UNMATCHED_TERMINATIONS)
                .map(e -> "UNMATCHED_TERMINATION " + e)
                .print();

        WatermarkStrategy<BurnRateUpdate> deltaWatermarks = WatermarkStrategy
                .<BurnRateUpdate>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkBoundSeconds))
                .withTimestampAssigner((update, recordTimestamp) -> update.eventTimeMillis)
                .withIdleness(Duration.ofSeconds(10));

        DataStream<BurnRateUpdate> timedBurnRateUpdates =
                burnRateUpdates.assignTimestampsAndWatermarks(deltaWatermarks);

        SingleOutputStreamOperator<WindowDelta> windowDeltas = timedBurnRateUpdates
                .keyBy(u -> u.accountId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(Duration.ofSeconds(allowedLatenessSeconds))
                .sideOutputLateData(LATE_BURN_RATE_UPDATES)
                .process(new WindowDeltaAggregator());

        windowDeltas.getSideOutput(LATE_BURN_RATE_UPDATES)
                .map(e -> "LATE: " + e)
                .print();

        DataStream<RunningRateUpdate> runningRates = windowDeltas
                .keyBy(d -> d.accountId)
                .process(new RunningRateFunction());

        runningRates.print();

        env.execute("flink-cloud-burn-rate");
    }
}
