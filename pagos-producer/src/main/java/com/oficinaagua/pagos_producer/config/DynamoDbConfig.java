package com.oficinaagua.pagos_producer.config;

import com.oficinaagua.pagos_producer.domain.entity.dynamo.PaymentLog;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.EnhancedGlobalSecondaryIndex;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

import java.net.URI;

@Configuration
@Slf4j
public class DynamoDbConfig {

    private static final ProvisionedThroughput THROUGHPUT = ProvisionedThroughput.builder()
            .readCapacityUnits(5L)
            .writeCapacityUnits(5L)
            .build();

    @Bean
    public DynamoDbClient dynamoDbClient(DynamoProperties props) {
        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .region(Region.of(props.region()));

        if (StringUtils.hasText(props.endpoint())) {
            builder.endpointOverride(URI.create(props.endpoint()));
        }
        if (StringUtils.hasText(props.accessKey())) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(props.accessKey(), props.secretKey())));
        }
        return builder.build();
    }

    @Bean
    public DynamoDbEnhancedClient dynamoDbEnhancedClient(DynamoDbClient client) {
        return DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();
    }

    @Bean
    public DynamoDbTable<PaymentLog> paymentLogTable(DynamoDbEnhancedClient enhancedClient,
                                                     DynamoProperties props) {
        DynamoDbTable<PaymentLog> table = enhancedClient.table(
                props.tableName(), TableSchema.fromBean(PaymentLog.class));

        if (props.createTable()) {
            createTableIfMissing(table);
        }
        return table;
    }

    private void createTableIfMissing(DynamoDbTable<PaymentLog> table) {
        try {
            table.describeTable();
        } catch (ResourceNotFoundException e) {
            table.createTable(request -> request
                    .provisionedThroughput(THROUGHPUT)
                    .globalSecondaryIndices(EnhancedGlobalSecondaryIndex.builder()
                            .indexName(PaymentLog.STATUS_INDEX)
                            .projection(p -> p.projectionType(ProjectionType.ALL))
                            .provisionedThroughput(THROUGHPUT)
                            .build()));
            log.info("Tabla DynamoDB creada: {}", table.tableName());
        } catch (Exception e) {
            // Los logs son de apoyo: si DynamoDB no está disponible, la app debe arrancar igual
            log.warn("No se pudo verificar ni crear la tabla DynamoDB: {}", e.getMessage());
        }
    }
}