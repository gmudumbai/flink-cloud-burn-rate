package com.example.burnrate.model;

/** Net sum of deltaHourlyRate for one account within one 1-minute tumbling window. */
public class WindowDelta {
    public String accountId;
    public long windowEndMillis;
    public double netDeltaHourlyRate;

    public WindowDelta() {
    }

    public WindowDelta(String accountId, long windowEndMillis, double netDeltaHourlyRate) {
        this.accountId = accountId;
        this.windowEndMillis = windowEndMillis;
        this.netDeltaHourlyRate = netDeltaHourlyRate;
    }

    @Override
    public String toString() {
        return String.format("WindowDelta{account=%s, windowEnd=%d, net=%.4f}",
                accountId, windowEndMillis, netDeltaHourlyRate);
    }
}
