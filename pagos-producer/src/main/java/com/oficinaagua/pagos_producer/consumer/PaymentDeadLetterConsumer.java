package com.oficinaagua.pagos_producer.consumer;

import com.oficinaagua.pagos_producer.config.ErpProperties;
import com.oficinaagua.pagos_producer.domain.dto.PaymentEventMsg;
import com.oficinaagua.pagos_producer.domain.entity.dynamo.PaymentDestination;
import com.oficinaagua.pagos_producer.domain.entity.dynamo.PaymentLogStatus;
import com.oficinaagua.pagos_producer.service.PaymentLogService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentDeadLetterConsumer {

    private final PaymentLogService paymentLogService;
    private final ErpProperties erpProperties;

    @KafkaListener(
            topics = "${kafka.topic.bank-payments-dlt}",
            groupId = "${spring.kafka.consumer.group-id}-dlt")
    public void onDeadLetter(PaymentEventMsg event,
                             @Header(KafkaHeaders.DLT_EXCEPTION_MESSAGE) String reason,
                             @Header(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP) String originalGroup) {
        PaymentDestination destination = erpProperties.consumerGroup().equals(originalGroup)
                ? PaymentDestination.ERP
                : PaymentDestination.BANK;

        // Quita el prefijo "Listener method ... threw exception; " para quedarse con la causa real
        int separator = reason.indexOf("; ");
        String cleanReason = separator >= 0 ? reason.substring(separator + 2) : reason;

        log.error("PAGO EN DEAD-LETTER {} ({}): {}. Requiere revisión manual.",
                event.getPaymentId(), destination, cleanReason);
        paymentLogService.logFailure(destination, event, PaymentLogStatus.DEAD_LETTER, null, null, cleanReason);
    }
}