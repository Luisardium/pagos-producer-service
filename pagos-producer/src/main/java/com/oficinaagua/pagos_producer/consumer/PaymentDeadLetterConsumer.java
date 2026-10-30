package com.oficinaagua.pagos_producer.consumer;

import com.oficinaagua.pagos_producer.domain.dto.PaymentEventMsg;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PaymentDeadLetterConsumer {

    @KafkaListener(
            topics = "${kafka.topic.bank-payments}.DLT",
            groupId = "${spring.kafka.consumer.group-id}-dlt")
    public void onDeadLetter(PaymentEventMsg event,
                             @Header(KafkaHeaders.DLT_EXCEPTION_MESSAGE) String reason) {
        log.error("PAGO EN DEAD-LETTER {}: {}. Requiere revisión manual.", event.getPaymentId(), reason);
    }
}