package com.oficinaagua.pagos_producer.exception;

public class NonRetryablePaymentException extends RuntimeException {
    public NonRetryablePaymentException(String msg) { super(msg); }
}
