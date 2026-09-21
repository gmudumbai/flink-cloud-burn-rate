package com.example.burnrate.model;

import com.fasterxml.jackson.databind.ObjectMapper;

/** A live price change for one instance type, read from the pricing-updates topic. */
public class PriceUpdate {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String instanceType;
    public double hourlyRate;

    public PriceUpdate() {
    }

    public PriceUpdate(String instanceType, double hourlyRate) {
        this.instanceType = instanceType;
        this.hourlyRate = hourlyRate;
    }

    public static PriceUpdate parse(String json) throws Exception {
        return MAPPER.readValue(json, PriceUpdate.class);
    }

    @Override
    public String toString() {
        return String.format("PriceUpdate{instanceType=%s, hourlyRate=%.4f}", instanceType, hourlyRate);
    }
}
