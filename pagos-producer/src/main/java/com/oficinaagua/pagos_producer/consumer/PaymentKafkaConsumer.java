package com.oficinaagua.pagos_producer.consumer;

import com.oficinaagua.pagos_producer.banco.api.BankPaymentsApiClient;
import com.oficinaagua.pagos_producer.banco.model.BankPaymentReceipt;
import com.oficinaagua.pagos_producer.banco.model.BankPaymentRequest;
import com.oficinaagua.pagos_producer.config.BankProperties;
import com.oficinaagua.pagos_producer.config.CorrelationIdInterceptor;
import com.oficinaagua.pagos_producer.domain.dto.PaymentEventMsg;
import com.oficinaagua.pagos_producer.exception.NonRetryablePaymentException;
import com.oficinaagua.pagos_producer.exception.RetryablePaymentException;
import com.oficinaagua.pagos_producer.mapper.BankPaymentMapper;
import com.oficinaagua.pagos_producer.service.PaymentStatusService;
import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentKafkaConsumer {

    private final BankPaymentsApiClient bankClient;
    private final BankPaymentMapper mapper;
    private final BankErrorClassifier errorClassifier;
    private final PaymentStatusService statusService;
    private final BankProperties bankProperties;

    @KafkaListener(topics = "${kafka.topic.bank-payments}")
    public void consume(PaymentEventMsg event) {
        String key = event.getPaymentId();
        MDC.put(CorrelationIdInterceptor.MDC_KEY, key);
        try {
            BankPaymentRequest request = mapper.toBankRequest(event);
            BankPaymentReceipt receipt = bankClient
                    .sendBankPayment(key, request, key, bankProperties.simulationScenario())
                    .getBody();

            if (receipt == null) {
                // Reintentar es seguro: si el banco sí lo registró, responderá DUPLICATE_PAYMENT
                throw new RetryablePaymentException("Respuesta vacía del banco para el pago " + key);
            }

            statusService.markSent(key, receipt.getReceiptId());
            log.info("Pago {} registrado en el banco. Comprobante {}", key, receipt.getReceiptId());
        } catch (FeignException e) {
            handleBankError(key, e);
        } finally {
            MDC.remove(CorrelationIdInterceptor.MDC_KEY);
        }
    }

    private void handleBankError(String key, FeignException e) {
        switch (errorClassifier.classify(e)) {
            case ALREADY_DELIVERED -> {
                statusService.markSent(key, errorClassifier.originalReceiptId(e));
                log.info("Pago {} ya estaba en el banco, se marca como entregado", key);
            }
            case TRANSIENT -> throw new RetryablePaymentException(
                    "Fallo transitorio (" + e.status() + ") en pago " + key, e);
            case REJECTED -> throw new NonRetryablePaymentException(
                    "Banco rechazó el pago " + key + " con estado " + e.status());
        }
    }
}