package com.oficinaagua.pagos_producer.mapper;

import com.oficinaagua.pagos_producer.config.ErpProperties;
import com.oficinaagua.pagos_producer.domain.dto.PaymentEventMsg;
import com.oficinaagua.pagos_producer.erp.model.ErpContribuyente;
import com.oficinaagua.pagos_producer.erp.model.ErpDocumentoOrigen;
import com.oficinaagua.pagos_producer.erp.model.ErpEquipo;
import com.oficinaagua.pagos_producer.erp.model.ErpLineaDetalle;
import com.oficinaagua.pagos_producer.erp.model.ErpPeriodo;
import com.oficinaagua.pagos_producer.erp.model.ErpRegistroPago;
import com.oficinaagua.pagos_producer.exception.NonRetryablePaymentException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

@Component
@RequiredArgsConstructor
public class ErpPaymentMapper {

    private static final int AMOUNT_DECIMALS = 2;

    private final ErpProperties erpProperties;

    public ErpRegistroPago toErpRequest(PaymentEventMsg event) {
        String total = toErpAmount(event.getAmountQuetzales());

        ErpDocumentoOrigen document = new ErpDocumentoOrigen(
                ErpDocumentoOrigen.TipoEnum.RECIBO_CAJA,
                event.getPaymentId(),                      // número estable: nunca uno nuevo por intento
                event.getTransactionDate().toLocalDate());

        ErpContribuyente taxpayer = new ErpContribuyente(event.getCustomerCode())
                .nit(event.getTaxId());

        ErpPeriodo period = new ErpPeriodo(
                event.getTransactionDate().getYear(),
                event.getTransactionDate().getMonthValue());

        ErpLineaDetalle line = new ErpLineaDetalle(ErpLineaDetalle.RubroEnum.AGUA_POTABLE, total);

        return new ErpRegistroPago(
                ErpEquipo.fromValue(erpProperties.teamCode()),
                document,
                taxpayer,
                period,
                ErpRegistroPago.FormaPagoEnum.EFECTIVO, //Esto deberia salir de la BD
                event.getTransactionDate(),
                total,
                List.of(line));
    }

    // El ERP exige texto con exactamente 2 decimales: 150.7 -> "150.70". Más de 2 decimales es inválido.
    private String toErpAmount(BigDecimal amount) {
        try {
            return amount.setScale(AMOUNT_DECIMALS, RoundingMode.UNNECESSARY).toPlainString();
        } catch (ArithmeticException ex) {
            throw new NonRetryablePaymentException("Monto inválido: " + amount);
        }
    }
}