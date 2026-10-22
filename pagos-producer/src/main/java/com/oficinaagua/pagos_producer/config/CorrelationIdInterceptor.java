package com.oficinaagua.pagos_producer.config;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class CorrelationIdInterceptor implements RequestInterceptor {

    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";

    @Override
    public void apply(RequestTemplate template) {
        // Si quien llama ya lo mandó, no lo duplicamos
        if (template.headers().containsKey(HEADER)) {
            return;
        }

        String id = MDC.get(MDC_KEY);
        if (id == null || id.isBlank()) {
            id = UUID.randomUUID().toString();
        }

        template.header(HEADER, id);
    }
}