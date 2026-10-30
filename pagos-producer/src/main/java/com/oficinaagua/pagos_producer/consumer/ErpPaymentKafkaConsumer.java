package com.oficinaagua.pagos_producer.consumer;

import com.oficinaagua.pagos_producer.config.CorrelationIdInterceptor;
import com.oficinaagua.pagos_producer.config.ErpProperties;
import com.oficinaagua.pagos_producer.domain.dto.PaymentEventMsg;
import com.oficinaagua.pagos_producer.domain.entity.dynamo.PaymentDestination;
import com.oficinaagua.pagos_producer.domain.entity.dynamo.PaymentLogStatus;
import com.oficinaagua.pagos_producer.erp.api.ErpPagosApiClient;
import com.oficinaagua.pagos_producer.erp.model.ErpComprobante;
import com.oficinaagua.pagos_producer.erp.model.ErpRegistroPago;
import com.oficinaagua.pagos_producer.exception.NonRetryablePaymentException;
import com.oficinaagua.pagos_producer.exception.RetryablePaymentException;
import com.oficinaagua.pagos_producer.mapper.ErpPaymentMapper;
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
public class ErpPaymentKafkaConsumer {

    private final ErpPagosApiClient erpClient;
    private final ErpPaymentMapper mapper;
    private final ErpErrorClassifier errorClassifier;
    private final PaymentStatusService statusService;
    private final PaymentLogService paymentLogService;
    private final ErpProperties erpProperties;

    // Mismo topic que el banco pero otro grupo: cada grupo recibe todos los pagos y avanza por su cuenta
    @KafkaListener(
            topics = "${kafka.topic.bank-payments}",
            groupId = "${erp.consumer-group:pagos-erp-consumer}")
    public void consume(PaymentEventMsg event) {
        MDC.put(CorrelationIdInterceptor.MDC_KEY, event.getPaymentId());
        try {
            processPayment(event);
        } catch (RetryablePaymentException e) {
            paymentLogService.logFailure(PaymentDestination.ERP, event, PaymentLogStatus.RETRY,
                    e.getHttpStatus(), e.getErrorCode(), e.getMessage());
            throw e;
        } catch (NonRetryablePaymentException e) {
            paymentLogService.logFailure(PaymentDestination.ERP, event, PaymentLogStatus.FAILED,
                    e.getHttpStatus(), e.getErrorCode(), e.getMessage());
            throw e;
        } finally {
            MDC.remove(CorrelationIdInterceptor.MDC_KEY);
        }
    }

    private void processPayment(PaymentEventMsg event) {
        String key = event.getPaymentId();
        try {
            ErpRegistroPago request = mapper.toErpRequest(event);
            ErpComprobante receipt = erpClient
                    .registrarPagoErp(request, key, erpProperties.simulationScenario())
                    .getBody();

            if (receipt == null) {
                // Reintentar es seguro: si el ERP sí lo registró, responderá DOCUMENTO_DUPLICADO
                throw new RetryablePaymentException("Respuesta vacía del ERP para el pago " + key);
            }

            statusService.markSentToErp(key, receipt.getNumeroComprobante());
            paymentLogService.logSuccess(PaymentDestination.ERP, event,
                    receipt.getNumeroComprobante(), "Pago contabilizado en el ERP");
            log.info("Pago {} contabilizado en el ERP. Comprobante {}", key, receipt.getNumeroComprobante());
        } catch (FeignException e) {
            handleErpError(event, e);
        }
    }

    private void handleErpError(PaymentEventMsg event, FeignException e) {
        String key = event.getPaymentId();
        switch (errorClassifier.classify(e)) {
            case ALREADY_DELIVERED -> {
                String receiptId = errorClassifier.originalReceiptId(e);
                statusService.markSentToErp(key, receiptId);
                paymentLogService.logSuccess(PaymentDestination.ERP, event, receiptId,
                        "El ERP ya tenía el pago (DOCUMENTO_DUPLICADO)");
                log.info("Pago {} ya estaba en el ERP, se marca como entregado", key);
            }
            case TRANSIENT -> throw new RetryablePaymentException(
                    "Fallo transitorio (" + e.status() + ") en pago " + key + ": "
                            + errorClassifier.errorMessage(e),
                    e, e.status(), errorClassifier.errorCode(e));
            case REJECTED -> throw new NonRetryablePaymentException(
                    "ERP rechazó el pago " + key + " con estado " + e.status() + ": "
                            + errorClassifier.errorMessage(e),
                    e.status(), errorClassifier.errorCode(e));
        }
    }
}