package com.example.burnrate.model;

/**
 * Emitted whenever an account's committed hourly spend changes.
 * A Flink POJO: public fields, no-arg constructor, so serialization is efficient.
 */
public class BurnRateUpdate {
    public String accountId;
    public String team;
    public double deltaHourlyRate;
    public long eventTimeMillis;

    public BurnRateUpdate() {
    }

    public BurnRateUpdate(String accountId, String team, double deltaHourlyRate, long eventTimeMillis) {
        this.accountId = accountId;
        this.team = team;
        this.deltaHourlyRate = deltaHourlyRate;
        this.eventTimeMillis = eventTimeMillis;
    }

    @Override
    public String toString() {
        String sign = deltaHourlyRate >= 0 ? "+" : "";
        return String.format("%s%.4f account=%s team=%s", sign, deltaHourlyRate, accountId, team);
    }
}
