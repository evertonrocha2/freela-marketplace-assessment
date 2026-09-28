package br.com.freela.contrato.infrastructure.persistence;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

interface SpringDataContratoRepository extends JpaRepository<ContratoJpaEntity, UUID> {

    /**
     * Le o contrato com lock exclusivo, serializando alteracoes concorrentes no mesmo contrato.
     *
     * <p>Sem isso, dois requests simultaneos sobre o mesmo contrato poderiam ler o mesmo estado e
     * gravar eventos cuja ordem na outbox nao corresponde a ordem real das transicoes.</p>
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select c from ContratoJpaEntity c where c.id = :id")
    Optional<ContratoJpaEntity> buscarParaAtualizacao(@Param("id") UUID id);
}
