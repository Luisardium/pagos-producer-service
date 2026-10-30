package com.oficinaagua.pagos_producer.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feign.FeignException;
import org.springframework.stereotype.Component;

@Component
public class BankErrorClassifier {

    private static final int NO_RESPONSE = -1;   // Feign usa -1 en timeouts y errores de conexión
    private static final int CONFLICT = 409;
    private static final int TOO_MANY_REQUESTS = 429;
    private static final int SERVER_ERROR = 500;
    private static final String CODE_DUPLICATE_PAYMENT = "DUPLICATE_PAYMENT";

    private final ObjectMapper json = new ObjectMapper();

    public DeliveryOutcome classify(FeignException e) {
        int status = e.status();

        if (status == CONFLICT
                && CODE_DUPLICATE_PAYMENT.equals(readBody(e).path("code").asText())) {
            return DeliveryOutcome.ALREADY_DELIVERED;
        }
        if (status == NO_RESPONSE || status == TOO_MANY_REQUESTS || status >= SERVER_ERROR) {
            return DeliveryOutcome.TRANSIENT;
        }
        return DeliveryOutcome.REJECTED;
    }

    public String originalReceiptId(FeignException e) {
        return readBody(e).path("originalReceipt").path("receiptId").asText(null);
    }
    public String errorCode(FeignException e) {
        String code = readBody(e).path("code").asText(null);
        if (code == null && e.status() == NO_RESPONSE) {
            return "NO_RESPONSE";
        }
        return code;
    }

    public String errorMessage(FeignException e) {
        String message = readBody(e).path("message").asText(null);
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