package com.oficinaagua.pagos_producer.exception;

import lombok.Getter;

@Getter
public class NonRetryablePaymentException extends RuntimeException {

    private final Integer httpStatus;
    private final String errorCode;

    public NonRetryablePaymentException(String msg) {
        this(msg, null, null);
    }

    public NonRetryablePaymentException(String msg, Integer httpStatus, String errorCode) {
        super(msg);
        this.httpStatus = httpStatus;
        this.errorCode = errorCode;
    }
}