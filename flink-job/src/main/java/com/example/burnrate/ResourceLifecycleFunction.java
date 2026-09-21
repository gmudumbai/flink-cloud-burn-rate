package com.example.burnrate;

import com.example.burnrate.model.BurnRateUpdate;
import com.example.burnrate.model.InstanceLifecycleEvent;
import com.example.burnrate.model.PriceUpdate;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Keyed by instanceId, connected to a broadcast stream of live pricing updates.
 * Tracks whether an instance is currently running and emits a BurnRateUpdate
 * whenever that changes. The rate is captured at RunInstances time and stored in
 * state, so a mid-flight price change never causes RunInstances/TerminateInstances
 * to disagree on what an instance cost. A TerminateInstances with no matching prior
 * RunInstances is routed to the unmatchedTerminations side output instead of
 * guessing a rate to subtract.
 */
public class ResourceLifecycleFunction
        extends KeyedBroadcastProcessFunction<String, InstanceLifecycleEvent, PriceUpdate, BurnRateUpdate> {

    public static final MapStateDescriptor<String, Double> PRICING_STATE =
            new MapStateDescriptor<>("pricing-state", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO);

    public static final OutputTag<InstanceLifecycleEvent> UNMATCHED_TERMINATIONS =
            new OutputTag<InstanceLifecycleEvent>("unmatched-terminations") {
            };

    private transient ValueState<RunningInstance> state;
    // Broadcast state isn't guaranteed to be populated before the first keyed
    // element is processed (no ordering guarantee between the two inputs), so
    // this static default covers that startup race and any instance type a
    // price was never broadcast for.
    private final PricingLookup fallbackPricing;

    public ResourceLifecycleFunction(PricingLookup fallbackPricing) {
        this.fallbackPricing = fallbackPricing;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<RunningInstance> descriptor =
                new ValueStateDescriptor<>("running-instance", RunningInstance.class);
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofHours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
        descriptor.enableTimeToLive(ttlConfig);
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(InstanceLifecycleEvent event, ReadOnlyContext ctx, Collector<BurnRateUpdate> out)
            throws Exception {
        RunningInstance current = state.value();

        if ("RunInstances".equals(event.eventName)) {
            if (current != null) {
                // Duplicate start for an instance we already think is running; ignore.
                return;
            }
            ReadOnlyBroadcastState<String, Double> pricing = ctx.getBroadcastState(PRICING_STATE);
            Double rate = pricing.get(event.instanceType);
            if (rate == null) {
                rate = fallbackPricing.hourlyRate(event.instanceType);
            }
            state.update(new RunningInstance(event.instanceType, event.accountId, event.team, event.eventTimeMillis, rate));
            out.collect(new BurnRateUpdate(event.accountId, event.team, rate, event.eventTimeMillis));

        } else if ("TerminateInstances".equals(event.eventName)) {
            if (current == null) {
                ctx.output(UNMATCHED_TERMINATIONS, event);
                return;
            }
            state.clear();
            // Subtract the rate captured at start time, not whatever the price is
            // now -- otherwise a mid-flight price change would make starts and
            // stops disagree and the account's running total would drift.
            out.collect(new BurnRateUpdate(current.accountId, current.team, -current.hourlyRateAtStart, event.eventTimeMillis));
        }
    }

    @Override
    public void processBroadcastElement(PriceUpdate update, Context ctx, Collector<BurnRateUpdate> out)
            throws Exception {
        ctx.getBroadcastState(PRICING_STATE).put(update.instanceType, update.hourlyRate);
    }

    /** State POJO: public fields, no-arg constructor. */
    public static class RunningInstance {
        public String instanceType;
        public String accountId;
        public String team;
        public long startTimeMillis;
        public double hourlyRateAtStart;

        public RunningInstance() {
        }

        public RunningInstance(String instanceType, String accountId, String team,
                                long startTimeMillis, double hourlyRateAtStart) {
            this.instanceType = instanceType;
            this.accountId = accountId;
            this.team = team;
            this.startTimeMillis = startTimeMillis;
            this.hourlyRateAtStart = hourlyRateAtStart;
        }
    }
}
