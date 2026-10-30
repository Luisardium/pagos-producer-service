package com.oficinaagua.pagos_producer.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feign.FeignException;
import org.springframework.stereotype.Component;

@Component
public class ErpErrorClassifier {

    private static final int NO_RESPONSE = -1;   // Feign usa -1 en timeouts y errores de conexión
    private static final int CONFLICT = 409;
    private static final int TOO_MANY_REQUESTS = 429;
    private static final int SERVER_ERROR = 500;
    private static final String CODE_DUPLICATE = "DOCUMENTO_DUPLICADO";

    private final ObjectMapper json = new ObjectMapper();

    public DeliveryOutcome classify(FeignException e) {
        int status = e.status();

        if (status == CONFLICT && CODE_DUPLICATE.equals(readBody(e).path("codigo_error").asText())) {
            return DeliveryOutcome.ALREADY_DELIVERED;
        }
        if (status == NO_RESPONSE || status == TOO_MANY_REQUESTS || status >= SERVER_ERROR) {
            return DeliveryOutcome.TRANSIENT;
        }
        return DeliveryOutcome.REJECTED;
    }

    public String originalReceiptId(FeignException e) {
        return readBody(e).path("comprobante_original").path("numero_comprobante").asText(null);
    }

    public String errorCode(FeignException e) {
        String code = readBody(e).path("codigo_error").asText(null);
        return (code == null && e.status() == NO_RESPONSE) ? "NO_RESPONSE" : code;
    }

    public String errorMessage(FeignException e) {
        JsonNode body = readBody(e);
        String message = body.path("mensaje").asText(null);
        if (message == null) {
            message = body.path("message").asText(null);   // errores de API Gateway
        }
        return message != null ? message : e.getMessage();
    }

    private JsonNode readBody(FeignException e) {
        try {
            return json.readTree(e.contentUTF8());
        } catch (Exception ex) {
            return json.createObjectNode();
        }
    }
}