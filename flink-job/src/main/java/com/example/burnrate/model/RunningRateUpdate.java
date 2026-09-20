package com.example.burnrate.model;

import java.time.Instant;

/** The running committed hourly rate for one account, as of one window's end. */
public class RunningRateUpdate {
    public String accountId;
    public long windowEndMillis;
    public double runningHourlyRate;

    public RunningRateUpdate() {
    }

    public RunningRateUpdate(String accountId, long windowEndMillis, double runningHourlyRate) {
        this.accountId = accountId;
        this.windowEndMillis = windowEndMillis;
        this.runningHourlyRate = runningHourlyRate;
    }

    @Override
    public String toString() {
        return String.format("account=%s windowEnd=%s runningRate=$%.4f/hr",
                accountId, Instant.ofEpochMilli(windowEndMillis), runningHourlyRate);
    }
}
