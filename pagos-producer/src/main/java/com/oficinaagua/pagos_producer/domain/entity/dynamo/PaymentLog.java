package com.oficinaagua.pagos_producer.domain.entity.dynamo;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import java.math.BigDecimal;

@DynamoDbBean
@Getter @Setter
@NoArgsConstructor
public class PaymentLog {

    public static final String STATUS_INDEX = "status-index";

    @Getter(onMethod_ = @DynamoDbPartitionKey)
    private String paymentId;           // idempotency key del pago

    @Getter(onMethod_ = {@DynamoDbSortKey, @DynamoDbSecondarySortKey(indexNames = STATUS_INDEX)})
    private String eventTime;           // fecha del evento, ISO-8601 en UTC

    @Getter(onMethod_ = @DynamoDbSecondaryPartitionKey(indexNames = STATUS_INDEX))
    private String status;              // SUCCESS, RETRY, FAILED, DEAD_LETTER
    private String destination;         // BANK o ERP

    private Integer httpStatus;         // estado HTTP del banco (-1 = sin respuesta)
    private String errorCode;           // código del banco, ej: AMOUNT_LIMIT_EXCEEDED
    private String reason;              // razón del fallo, o detalle si fue exitoso
    private String receiptId;           // comprobante del banco
    private BigDecimal amountQuetzales;
    private String customerCode;
}