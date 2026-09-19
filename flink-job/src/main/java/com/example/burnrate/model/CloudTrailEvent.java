package com.example.burnrate.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses a raw CloudTrail JSON record into a list of per-instance lifecycle
 * events (one per item in responseElements.instancesSet.items[]).
 */
public final class CloudTrailEvent {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CloudTrailEvent() {
    }

    public static List<InstanceLifecycleEvent> parse(String json) throws Exception {
        JsonNode root = MAPPER.readTree(json);

        String eventName = root.path("eventName").asText();
        long eventTimeMillis = Instant.parse(root.path("eventTime").asText()).toEpochMilli();
        String accountId = root.path("recipientAccountId").asText();
        String team = root.path("tags").path("team").asText(null);

        List<InstanceLifecycleEvent> events = new ArrayList<>();
        JsonNode items = root.path("responseElements").path("instancesSet").path("items");
        for (JsonNode item : items) {
            String instanceId = item.path("instanceId").asText();
            String instanceType = item.path("instanceType").asText();
            events.add(new InstanceLifecycleEvent(
                    eventName, eventTimeMillis, accountId, team, instanceId, instanceType));
        }
        return events;
    }
}
