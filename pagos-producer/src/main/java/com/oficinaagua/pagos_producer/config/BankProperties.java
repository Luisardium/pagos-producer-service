package com.oficinaagua.pagos_producer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "bank")
public record BankProperties(
        String teamCode,
        @DefaultValue("AUTO") String simulationScenario,
        @DefaultValue("pagos-producer") String sourceSystem) {
}