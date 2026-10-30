package com.oficinaagua.pagos_producer.consumer;

import com.oficinaagua.pagos_producer.banco.api.BankPaymentsApiClient;
import com.oficinaagua.pagos_producer.banco.model.BankPaymentReceipt;
import com.oficinaagua.pagos_producer.banco.model.BankPaymentRequest;
import com.oficinaagua.pagos_producer.config.BankProperties;
import com.oficinaagua.pagos_producer.config.CorrelationIdInterceptor;
import com.oficinaagua.pagos_producer.domain.dto.PaymentEventMsg;
import com.oficinaagua.pagos_producer.domain.entity.dynamo.PaymentLogStatus;
import com.oficinaagua.pagos_producer.exception.NonRetryablePaymentException;
import com.oficinaagua.pagos_producer.exception.RetryablePaymentException;
import com.oficinaagua.pagos_producer.mapper.BankPaymentMapper;
import com.oficinaagua.pagos_producer.service.PaymentLogService;
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
    private final PaymentLogService paymentLogService;
    private final BankProperties bankProperties;

    @KafkaListener(topics = "${kafka.topic.bank-payments}")
    public void consume(PaymentEventMsg event) {
        log.warn("Escenario de simulación en uso: {}", bankProperties.simulationScenario());
        MDC.put(CorrelationIdInterceptor.MDC_KEY, event.getPaymentId());
        try {
            processPayment(event);
        } catch (RetryablePaymentException e) {
            paymentLogService.logFailure(event, PaymentLogStatus.RETRY,
                    e.getHttpStatus(), e.getErrorCode(), e.getMessage());
            throw e;
        } catch (NonRetryablePaymentException e) {
            paymentLogService.logFailure(event, PaymentLogStatus.FAILED,
                    e.getHttpStatus(), e.getErrorCode(), e.getMessage());
            throw e;
        } finally {
            MDC.remove(CorrelationIdInterceptor.MDC_KEY);
        }
    }

    private void processPayment(PaymentEventMsg event) {
        String key = event.getPaymentId();
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
            paymentLogService.logSuccess(event, receipt.getReceiptId(), "Pago registrado en el banco");
            log.info("Pago {} registrado en el banco. Comprobante {}", key, receipt.getReceiptId());
        } catch (FeignException e) {
            handleBankError(event, e);
        }
    }

    private void handleBankError(PaymentEventMsg event, FeignException e) {
        String key = event.getPaymentId();
        switch (errorClassifier.classify(e)) {
            case ALREADY_DELIVERED -> {
                String receiptId = errorClassifier.originalReceiptId(e);
                statusService.markSent(key, receiptId);
                paymentLogService.logSuccess(event, receiptId,
                        "El banco ya tenía el pago (DUPLICATE_PAYMENT)");
                log.info("Pago {} ya estaba en el banco, se marca como entregado", key);
            }
            case TRANSIENT -> throw new RetryablePaymentException(
                    "Fallo transitorio (" + e.status() + ") en pago " + key + ": "
                            + errorClassifier.errorMessage(e),
                    e, e.status(), errorClassifier.errorCode(e));
            case REJECTED -> throw new NonRetryablePaymentException(
                    "Banco rechazó el pago " + key + " con estado " + e.status() + ": "
                            + errorClassifier.errorMessage(e),
                    e.status(), errorClassifier.errorCode(e));
        }
    }
}