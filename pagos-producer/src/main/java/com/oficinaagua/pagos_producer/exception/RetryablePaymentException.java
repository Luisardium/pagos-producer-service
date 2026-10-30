package com.oficinaagua.pagos_producer.exception;

import lombok.Getter;

@Getter
public class RetryablePaymentException extends RuntimeException {

    private final Integer httpStatus;
    private final String errorCode;

    public RetryablePaymentException(String msg) {
        this(msg, null, null, null);
    }

    public RetryablePaymentException(String msg, Throwable cause, Integer httpStatus, String errorCode) {
        super(msg, cause);
        this.httpStatus = httpStatus;
        this.errorCode = errorCode;
    }
}