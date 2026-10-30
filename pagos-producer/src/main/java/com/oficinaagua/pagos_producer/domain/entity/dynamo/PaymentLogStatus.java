package com.oficinaagua.pagos_producer.domain.entity.dynamo;

public enum PaymentLogStatus {
    SUCCESS,      // el banco aceptó el pago (o ya lo tenía)
    RETRY,        // intento fallido por error temporal, se reintentará
    FAILED,       // rechazo definitivo, no se reintenta
    DEAD_LETTER   // el mensaje terminó en la dead-letter
}