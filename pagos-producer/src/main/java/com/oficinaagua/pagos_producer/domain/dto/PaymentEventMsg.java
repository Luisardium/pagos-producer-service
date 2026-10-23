package com.oficinaagua.pagos_producer.domain.dto;

import lombok.*;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEventMsg {
    private String paymentId;          // Idempotency Key (ej: EQB-000123)
    private String customerCode;
    private String customerName;
    private String taxId;
    private BigDecimal amountQuetzales;
    private OffsetDateTime transactionDate;
}