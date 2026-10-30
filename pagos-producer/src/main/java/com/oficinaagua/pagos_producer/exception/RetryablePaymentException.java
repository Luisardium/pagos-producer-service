package com.oficinaagua.pagos_producer.exception;

public class RetryablePaymentException extends RuntimeException {

    public RetryablePaymentException(String msg) { super(msg); }

    public RetryablePaymentException(String msg, Throwable cause) { super(msg, cause); }
}