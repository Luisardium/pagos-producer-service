package com.oficinaagua.pagos_producer.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feign.FeignException;
import org.springframework.stereotype.Component;

@Component
public class BankErrorClassifier {

    public enum Outcome {
        ALREADY_DELIVERED,  // el banco ya tiene el pago: se trata como éxito
        TRANSIENT,          // fallo temporal: se reintenta
        REJECTED            // error definitivo: va directo a dead-letter
    }

    private static final int NO_RESPONSE = -1;   // Feign usa -1 en timeouts y errores de conexión
    private static final int CONFLICT = 409;
    private static final int TOO_MANY_REQUESTS = 429;
    private static final int SERVER_ERROR = 500;
    private static final String CODE_DUPLICATE_PAYMENT = "DUPLICATE_PAYMENT";

    private final ObjectMapper json = new ObjectMapper();

    public Outcome classify(FeignException e) {
        int status = e.status();

        if (status == CONFLICT
                && CODE_DUPLICATE_PAYMENT.equals(readBody(e).path("code").asText())) {
            return Outcome.ALREADY_DELIVERED;
        }
        if (status == NO_RESPONSE || status == TOO_MANY_REQUESTS || status >= SERVER_ERROR) {
            return Outcome.TRANSIENT;
        }
        return Outcome.REJECTED;
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