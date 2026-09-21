package com.example.burnrate;

import com.example.burnrate.model.PriceUpdate;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads on-demand hourly rates from pricing/ec2-us-east-1.json (bundled on the classpath).
 */
public class PricingLookup implements Serializable {

    private final Map<String, Double> hourlyRates;

    public PricingLookup() {
        this.hourlyRates = load();
    }

    private static Map<String, Double> load() {
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream in = PricingLookup.class.getClassLoader()
                .getResourceAsStream("pricing/ec2-us-east-1.json")) {
            if (in == null) {
                throw new IllegalStateException("pricing/ec2-us-east-1.json not found on classpath");
            }
            Map<String, Object> raw = mapper.readValue(in, Map.class);
            Map<String, Double> rates = new HashMap<>();
            for (Map.Entry<String, Object> entry : raw.entrySet()) {
                if (entry.getKey().startsWith("_")) {
                    continue;
                }
                rates.put(entry.getKey(), ((Number) entry.getValue()).doubleValue());
            }
            return rates;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load EC2 pricing data", e);
        }
    }

    public double hourlyRate(String instanceType) {
        Double rate = hourlyRates.get(instanceType);
        if (rate == null) {
            throw new IllegalArgumentException("No known price for instance type: " + instanceType);
        }
        return rate;
    }

    /** Seeds the broadcast pricing state at job start (see BurnRateJob's pricing broadcast stream). */
    public List<PriceUpdate> initialPriceUpdates() {
        List<PriceUpdate> updates = new ArrayList<>();
        for (Map.Entry<String, Double> entry : hourlyRates.entrySet()) {
            updates.add(new PriceUpdate(entry.getKey(), entry.getValue()));
        }
        return updates;
    }
}
