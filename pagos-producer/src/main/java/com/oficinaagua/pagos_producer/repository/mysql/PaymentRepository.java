package com.oficinaagua.pagos_producer.repository.mysql;

import com.oficinaagua.pagos_producer.domain.entity.mysql.PaymentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PaymentRepository extends JpaRepository<PaymentEntity, Long> {

    // Consulta los pagos que aún no han sido enviados al banco ni puestos en cola
    List<PaymentEntity> findBySentToBankFalseAndEnqueuedFalse();

    Optional<PaymentEntity> findByIdempotencyKey(String idempotencyKey);
}