package com.oficinaagua.pagos_producer.consumer;

public enum DeliveryOutcome {
    ALREADY_DELIVERED,  // el destino ya tiene el pago: se trata como éxito
    TRANSIENT,          // fallo temporal: se reintenta
    REJECTED            // error definitivo: va directo a dead-letter
}