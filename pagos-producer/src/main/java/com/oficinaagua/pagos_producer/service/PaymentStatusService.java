package com.oficinaagua.pagos_producer.service;

import com.oficinaagua.pagos_producer.exception.NonRetryablePaymentException;
import com.oficinaagua.pagos_producer.repository.mysql.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class PaymentStatusService {

    private final PaymentRepository repository;

    @Transactional
    public void markSentToBank(String idempotencyKey, String receiptId) {
        requireUpdated(repository.markSentToBank(idempotencyKey, receiptId), idempotencyKey);
    }

    @Transactional
    public void markSentToErp(String idempotencyKey, String receiptId) {
        requireUpdated(repository.markSentToErp(idempotencyKey, receiptId), idempotencyKey);
    }

    private void requireUpdated(int rowsUpdated, String idempotencyKey) {
        if (rowsUpdated == 0) {
            throw new NonRetryablePaymentException("El pago " + idempotencyKey + " no existe en la BD");
        }
    }
}