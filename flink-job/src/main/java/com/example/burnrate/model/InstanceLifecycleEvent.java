package com.example.burnrate.model;

/**
 * One instance's slice of a CloudTrail event, after flattening
 * responseElements.instancesSet.items[] into per-instance records.
 * A Flink POJO: public fields, no-arg constructor.
 */
public class InstanceLifecycleEvent {
    public String eventName;
    public long eventTimeMillis;
    public String accountId;
    public String team;
    public String instanceId;
    public String instanceType;

    public InstanceLifecycleEvent() {
    }

    public InstanceLifecycleEvent(String eventName, long eventTimeMillis, String accountId,
                                   String team, String instanceId, String instanceType) {
        this.eventName = eventName;
        this.eventTimeMillis = eventTimeMillis;
        this.accountId = accountId;
        this.team = team;
        this.instanceId = instanceId;
        this.instanceType = instanceType;
    }

    @Override
    public String toString() {
        return "InstanceLifecycleEvent{" +
                "eventName='" + eventName + '\'' +
                ", eventTimeMillis=" + eventTimeMillis +
                ", accountId='" + accountId + '\'' +
                ", team='" + team + '\'' +
                ", instanceId='" + instanceId + '\'' +
                ", instanceType='" + instanceType + '\'' +
                '}';
    }
}
