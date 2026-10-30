package com.oficinaagua.pagos_producer.service;

import com.oficinaagua.pagos_producer.domain.entity.mysql.PaymentEntity;
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
    public void markSent(String idempotencyKey, String receiptId) {
        PaymentEntity payment = repository.findByIdempotencyKey(idempotencyKey)
                .orElseThrow(() -> new NonRetryablePaymentException(
                        "El pago " + idempotencyKey + " no existe en la BD"));

        payment.setSentToBank(true);
        payment.setBankReceiptId(receiptId);
    }
}