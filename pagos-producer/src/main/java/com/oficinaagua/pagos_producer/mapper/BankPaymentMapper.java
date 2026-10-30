package com.oficinaagua.pagos_producer.mapper;

import com.oficinaagua.pagos_producer.banco.model.BankPaymentRequest;
import com.oficinaagua.pagos_producer.banco.model.Money;
import com.oficinaagua.pagos_producer.banco.model.Payer;
import com.oficinaagua.pagos_producer.banco.model.TeamCode;
import com.oficinaagua.pagos_producer.config.BankProperties;
import com.oficinaagua.pagos_producer.domain.dto.PaymentEventMsg;
import com.oficinaagua.pagos_producer.exception.NonRetryablePaymentException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.math.RoundingMode;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class BankPaymentMapper {

    private static final String DESCRIPTION_PREFIX = "Pago servicio de agua ";
    private static final int CENTS_DECIMALS = 2;

    private final BankProperties bankProperties;

    public BankPaymentRequest toBankRequest(PaymentEventMsg event) {
        Payer payer = new Payer(event.getCustomerCode())
                .fullName(event.getCustomerName())
                .taxId(event.getTaxId());

        BankPaymentRequest request = new BankPaymentRequest(
                TeamCode.fromValue(bankProperties.teamCode()),
                payer,
                new Money(toCents(event), Money.CurrencyEnum.GTQ),
                event.getTransactionDate(),
                BankPaymentRequest.ChannelEnum.CASH);

        request.setDescription(DESCRIPTION_PREFIX + event.getPaymentId());
        request.setMetadata(Map.of("sourceSystem", bankProperties.sourceSystem()));
        return request;
    }

    // El banco exige centavos enteros: 150.75 -> 15075. Más de 2 decimales es un dato inválido.
    private long toCents(PaymentEventMsg event) {
        try {
            return event.getAmountQuetzales()
                    .movePointRight(CENTS_DECIMALS)
                    .setScale(0, RoundingMode.UNNECESSARY)
                    .longValueExact();
        } catch (ArithmeticException ex) {
            throw new NonRetryablePaymentException("Monto inválido: " + event.getAmountQuetzales());
        }
    }
}