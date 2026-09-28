package br.com.freela.contrato.infrastructure.outbox;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

public interface MensagemOutboxRepository extends JpaRepository<MensagemOutbox, Long> {

    /**
     * Proximas mensagens a publicar, em ordem de gravacao.
     *
     * <p>O lock com SKIP LOCKED (lock.timeout = -2 no Hibernate) permite rodar mais de uma
     * instancia do contrato-service: cada relay pega um conjunto distinto de linhas em vez de
     * disputar as mesmas. A ordem por sequencia continua valendo dentro de cada lote.</p>
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("select m from MensagemOutbox m where m.status = br.com.freela.contrato.infrastructure.outbox.StatusOutbox.PENDENTE order by m.sequencia asc")
    List<MensagemOutbox> buscarPendentes(Limit limite);

    List<MensagemOutbox> findByContratoIdOrderBySequenciaAsc(UUID contratoId);

    List<MensagemOutbox> findByStatusOrderBySequenciaAsc(StatusOutbox status);

    @Query("select m from MensagemOutbox m where m.correlationId = :correlationId order by m.sequencia asc")
    List<MensagemOutbox> buscarPorCorrelationId(@Param("correlationId") String correlationId);
}
