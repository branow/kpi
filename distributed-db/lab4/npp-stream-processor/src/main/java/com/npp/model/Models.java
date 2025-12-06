package com.npp.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Models {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Telemetry {
        public String unit_id;
        public long timestamp;
        public double neutron_flux;
        public double reactor_power;
        public double coolant_temp_inlet;
        public double coolant_temp_outlet;
        public double reactor_pressure;
        public int control_rod_position;
        public double radiation_level;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class EnrichedEvent {
        public Telemetry telemetry;
        public String eventType;
        public String description;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AggregatedState {
        public long count;
        public double sumNeutronFlux;
        public double avgEfficiency;
        public double safetyMargin;
        public double runningEfficiencySum;
        public long lastTimestamp;
    }
}
