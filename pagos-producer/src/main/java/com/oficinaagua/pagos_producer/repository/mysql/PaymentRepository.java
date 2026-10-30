package com.oficinaagua.pagos_producer.repository.mysql;

import com.oficinaagua.pagos_producer.domain.entity.mysql.PaymentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.Modifying;
import  org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

@Repository
public interface PaymentRepository extends JpaRepository<PaymentEntity, Long> {

    // Consulta los pagos que aún no han sido enviados al banco ni puestos en cola
    List<PaymentEntity> findBySentToBankFalseAndEnqueuedFalse();

    Optional<PaymentEntity> findByIdempotencyKey(String idempotencyKey);
    @Modifying
    @Query("UPDATE PaymentEntity p SET p.sentToBank = true, p.bankReceiptId = :receiptId WHERE p.idempotencyKey = :key")
    int markSentToBank(@Param("key") String key, @Param("receiptId") String receiptId);

    @Modifying
    @Query("UPDATE PaymentEntity p SET p.sentToErp = true, p.erpReceiptId = :receiptId WHERE p.idempotencyKey = :key")
    int markSentToErp(@Param("key") String key, @Param("receiptId") String receiptId);
}