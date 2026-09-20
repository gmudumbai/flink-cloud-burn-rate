package com.example.burnrate;

import com.example.burnrate.model.RunningRateUpdate;
import com.example.burnrate.model.WindowDelta;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed by accountId. Adds each window's net delta onto a running total and
 * emits the account's current committed hourly rate.
 */
public class RunningRateFunction extends KeyedProcessFunction<String, WindowDelta, RunningRateUpdate> {

    private transient ValueState<Double> runningRate;

    @Override
    public void open(Configuration parameters) {
        runningRate = getRuntimeContext().getState(
                new ValueStateDescriptor<>("running-rate", Double.class));
    }

    @Override
    public void processElement(WindowDelta delta, Context ctx, Collector<RunningRateUpdate> out) throws Exception {
        Double current = runningRate.value();
        double updated = (current == null ? 0.0 : current) + delta.netDeltaHourlyRate;
        runningRate.update(updated);
        out.collect(new RunningRateUpdate(delta.accountId, delta.windowEndMillis, updated));
    }
}
