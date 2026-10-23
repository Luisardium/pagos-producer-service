package com.oficinaagua.pagos_producer.producer;

import com.oficinaagua.pagos_producer.domain.dto.PaymentEventMsg;
import com.oficinaagua.pagos_producer.domain.entity.mysql.PaymentEntity;
import com.oficinaagua.pagos_producer.repository.mysql.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentKafkaProducer {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, PaymentEventMsg> kafkaTemplate;

    @Value("${kafka.topic.bank-payments}")
    private String bankTopic;

    @Scheduled(fixedDelay = 10000)
    @Transactional
    public void fetchAndPublishPendingPayments() {
        List<PaymentEntity> pendingPayments = paymentRepository.findBySentToBankFalseAndEnqueuedFalse();

        if (pendingPayments.isEmpty()) {
            return;
        }

        log.info("Se encontraron {} pagos pendientes de procesar.", pendingPayments.size());

        for (PaymentEntity payment : pendingPayments) {
            PaymentEventMsg event = PaymentEventMsg.builder()
                    .paymentId(payment.getIdempotencyKey())
                    .customerCode(payment.getCustomerCode())
                    .customerName(payment.getCustomerName())
                    .taxId(payment.getTaxId())
                    .amountQuetzales(payment.getAmount())
                    .transactionDate(payment.getCreatedAt())
                    .build();

            // Publica usando idempotencyKey como clave de particionamiento
            kafkaTemplate.send(bankTopic, event.getPaymentId(), event);
            log.info("Pago {} enviado a Kafka topic: {}", event.getPaymentId(), bankTopic);

            // Marca como encolado para evitar publicaciones duplicadas
            payment.setEnqueued(true);
            paymentRepository.save(payment);
        }
    }
}