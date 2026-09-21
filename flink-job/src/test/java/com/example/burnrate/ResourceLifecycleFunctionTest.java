package com.example.burnrate;

import com.example.burnrate.model.BurnRateUpdate;
import com.example.burnrate.model.InstanceLifecycleEvent;
import com.example.burnrate.model.PriceUpdate;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Feeds a RunInstances then a TerminateInstances for the same instance and
 * asserts a +rate then -rate BurnRateUpdate, using the same rate both times
 * even though no broadcast price was ever sent (exercises the fallback
 * PricingLookup path).
 */
class ResourceLifecycleFunctionTest {

    @Test
    void runThenTerminateEmitsMatchingPositiveThenNegativeDelta() throws Exception {
        List<MapStateDescriptor<?, ?>> broadcastStateDescriptors =
                Collections.singletonList(ResourceLifecycleFunction.PRICING_STATE);

        CoBroadcastWithKeyedOperator<String, InstanceLifecycleEvent, PriceUpdate, BurnRateUpdate> operator =
                new CoBroadcastWithKeyedOperator<>(
                        new ResourceLifecycleFunction(new PricingLookup()), broadcastStateDescriptors);

        KeyedBroadcastOperatorTestHarness<String, InstanceLifecycleEvent, PriceUpdate, BurnRateUpdate> harness =
                new KeyedBroadcastOperatorTestHarness<>(
                        operator,
                        (InstanceLifecycleEvent e) -> e.instanceId,
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(String.class),
                        1, 1, 0);

        harness.open();

        InstanceLifecycleEvent runEvent = new InstanceLifecycleEvent(
                "RunInstances", 1_000L, "acct-1", "platform", "i-test1", "m5.large");
        InstanceLifecycleEvent terminateEvent = new InstanceLifecycleEvent(
                "TerminateInstances", 2_000L, "acct-1", "platform", "i-test1", "m5.large");

        harness.processElement(new StreamRecord<>(runEvent, 1_000L));
        harness.processElement(new StreamRecord<>(terminateEvent, 2_000L));

        List<BurnRateUpdate> updates = new ArrayList<>();
        for (Object recordOrWatermark : harness.getOutput()) {
            if (recordOrWatermark instanceof StreamRecord) {
                //noinspection unchecked
                updates.add(((StreamRecord<BurnRateUpdate>) recordOrWatermark).getValue());
            } else if (!(recordOrWatermark instanceof Watermark)) {
                throw new IllegalStateException("Unexpected output: " + recordOrWatermark);
            }
        }

        assertEquals(2, updates.size(), "expected one update for the start and one for the stop");

        double m5LargeRate = new PricingLookup().hourlyRate("m5.large");
        assertEquals(m5LargeRate, updates.get(0).deltaHourlyRate, 1e-9, "RunInstances should add the rate");
        assertEquals(-m5LargeRate, updates.get(1).deltaHourlyRate, 1e-9, "TerminateInstances should subtract the same rate");
        assertEquals("acct-1", updates.get(0).accountId);
        assertEquals("acct-1", updates.get(1).accountId);
    }
}
