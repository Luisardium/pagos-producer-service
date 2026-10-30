package com.oficinaagua.pagos_producer.service;

import com.oficinaagua.pagos_producer.domain.dto.PaymentEventMsg;
import com.oficinaagua.pagos_producer.domain.entity.dynamo.PaymentLog;
import com.oficinaagua.pagos_producer.domain.entity.dynamo.PaymentLogStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentLogService {

    // Formato fijo (siempre con milisegundos) para que el orden alfabético sea el cronológico
    private static final DateTimeFormatter EVENT_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
    private static final int MAX_REASON_LENGTH = 500;

    private final DynamoDbTable<PaymentLog> table;

    public void logSuccess(PaymentEventMsg event, String receiptId, String detail) {
        PaymentLog entry = newEntry(event, PaymentLogStatus.SUCCESS);
        entry.setReceiptId(receiptId);
        entry.setReason(truncate(detail));
        save(entry);
    }

    public void logFailure(PaymentEventMsg event, PaymentLogStatus status,
                           Integer httpStatus, String errorCode, String reason) {
        PaymentLog entry = newEntry(event, status);
        entry.setHttpStatus(httpStatus);
        entry.setErrorCode(errorCode);
        entry.setReason(truncate(reason));
        save(entry);
    }

    private PaymentLog newEntry(PaymentEventMsg event, PaymentLogStatus status) {
        PaymentLog entry = new PaymentLog();
        entry.setPaymentId(event.getPaymentId());
        entry.setEventTime(EVENT_TIME.format(Instant.now()));
        entry.setStatus(status.name());
        entry.setAmountQuetzales(event.getAmountQuetzales());
        entry.setCustomerCode(event.getCustomerCode());
        return entry;
    }

    // Los logs son de apoyo: un fallo aquí nunca debe afectar el procesamiento del pago
    private void save(PaymentLog entry) {
        try {
            table.putItem(entry);
        } catch (Exception e) {
            log.error("No se pudo registrar en DynamoDB el pago {} ({}): {}",
                    entry.getPaymentId(), entry.getStatus(), e.getMessage());
        }
    }

    private String truncate(String text) {
        if (text == null || text.length() <= MAX_REASON_LENGTH) {
            return text;
        }
        return text.substring(0, MAX_REASON_LENGTH);
    }
}