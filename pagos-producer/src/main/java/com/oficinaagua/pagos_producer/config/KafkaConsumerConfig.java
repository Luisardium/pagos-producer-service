package com.oficinaagua.pagos_producer.config;

import com.oficinaagua.pagos_producer.exception.NonRetryablePaymentException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template) {
        // al agotar reintentos publica en "bank-payments-topic.DLT"
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(5);
        backOff.setInitialInterval(1000);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(15000);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // estos van directo a DLT sin reintentar
        handler.addNotRetryableExceptions(NonRetryablePaymentException.class);
        return handler;
    }
}