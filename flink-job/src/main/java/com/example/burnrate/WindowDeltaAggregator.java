package com.example.burnrate;

import com.example.burnrate.model.BurnRateUpdate;
import com.example.burnrate.model.WindowDelta;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** Sums deltaHourlyRate for all BurnRateUpdates in one account's 1-minute window. */
public class WindowDeltaAggregator
        extends ProcessWindowFunction<BurnRateUpdate, WindowDelta, String, TimeWindow> {

    @Override
    public void process(String accountId, Context context, Iterable<BurnRateUpdate> elements,
                         Collector<WindowDelta> out) {
        double net = 0.0;
        for (BurnRateUpdate update : elements) {
            net += update.deltaHourlyRate;
        }
        out.collect(new WindowDelta(accountId, context.window().getEnd(), net));
    }
}
