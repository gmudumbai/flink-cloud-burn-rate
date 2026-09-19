package com.example.burnrate;

import com.example.burnrate.model.BurnRateUpdate;
import com.example.burnrate.model.InstanceLifecycleEvent;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Keyed by instanceId. Tracks whether an instance is currently running and emits a
 * BurnRateUpdate whenever that changes. A TerminateInstances with no matching prior
 * RunInstances is routed to the unmatchedTerminations side output instead of guessing
 * a rate to subtract.
 */
public class ResourceLifecycleFunction
        extends KeyedProcessFunction<String, InstanceLifecycleEvent, BurnRateUpdate> {

    public static final OutputTag<InstanceLifecycleEvent> UNMATCHED_TERMINATIONS =
            new OutputTag<InstanceLifecycleEvent>("unmatched-terminations") {
            };

    private transient ValueState<RunningInstance> state;
    private final PricingLookup pricingLookup;

    public ResourceLifecycleFunction(PricingLookup pricingLookup) {
        this.pricingLookup = pricingLookup;
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
    public void processElement(InstanceLifecycleEvent event, Context ctx, Collector<BurnRateUpdate> out)
            throws Exception {
        RunningInstance current = state.value();

        if ("RunInstances".equals(event.eventName)) {
            if (current != null) {
                // Duplicate start for an instance we already think is running; ignore.
                return;
            }
            double rate = pricingLookup.hourlyRate(event.instanceType);
            state.update(new RunningInstance(event.instanceType, event.accountId, event.team, event.eventTimeMillis));
            out.collect(new BurnRateUpdate(event.accountId, event.team, rate, event.eventTimeMillis));

        } else if ("TerminateInstances".equals(event.eventName)) {
            if (current == null) {
                ctx.output(UNMATCHED_TERMINATIONS, event);
                return;
            }
            double rate = pricingLookup.hourlyRate(current.instanceType);
            state.clear();
            out.collect(new BurnRateUpdate(current.accountId, current.team, -rate, event.eventTimeMillis));
        }
    }

    /** State POJO: public fields, no-arg constructor. */
    public static class RunningInstance {
        public String instanceType;
        public String accountId;
        public String team;
        public long startTimeMillis;

        public RunningInstance() {
        }

        public RunningInstance(String instanceType, String accountId, String team, long startTimeMillis) {
            this.instanceType = instanceType;
            this.accountId = accountId;
            this.team = team;
            this.startTimeMillis = startTimeMillis;
        }
    }
}
