package com.oficinaagua.pagos_producer.domain.entity.mysql;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Entity
@Table(name = "pagos")
@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
/* TODO MODIFICAR LOS NOMBRES DE LAS COLUMNAS PARA QUE SEAN IGUALES A LOS DE LA BD DEL MONOLITO, Y AGREGAR CAMPOS
 AL MONOLITO SI ES NECESARIO */
public class PaymentEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Llave de idempotencia estable derivada del monolito (ej: EQA-000123)
    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Column(name = "customer_code", nullable = false)
    private String customerCode;

    @Column(name = "customer_name")
    private String customerName;

    @Column(name = "tax_id")
    private String taxId;

    @Column(name = "amount", nullable = false)
    private BigDecimal amount;

    @Column(name = "sent_to_bank", nullable = false)
    private Boolean sentToBank;

    @Column(name = "enqueued", nullable = false)
    private Boolean enqueued;

    @Column(name = "created_at")
    private OffsetDateTime createdAt;
}