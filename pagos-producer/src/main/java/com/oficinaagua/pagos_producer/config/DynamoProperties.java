package com.oficinaagua.pagos_producer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "dynamodb")
public record DynamoProperties(
        String endpoint,
        @DefaultValue("us-east-1") String region,
        String accessKey,
        String secretKey,
        @DefaultValue("payment_logs") String tableName,
        @DefaultValue("false") boolean createTable) {
}