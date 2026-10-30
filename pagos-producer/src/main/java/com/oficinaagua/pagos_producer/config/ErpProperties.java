package com.oficinaagua.pagos_producer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "erp")
public record ErpProperties(
        String teamCode,
        @DefaultValue("AUTO") String simulationScenario,
        @DefaultValue("pagos-erp-consumer") String consumerGroup) {
}