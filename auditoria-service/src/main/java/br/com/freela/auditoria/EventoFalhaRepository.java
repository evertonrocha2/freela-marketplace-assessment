package br.com.freela.auditoria;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EventoFalhaRepository extends JpaRepository<EventoFalha, UUID> {

    List<EventoFalha> findByContratoIdOrderByRecebidoEmAsc(UUID contratoId);

    List<EventoFalha> findByCorrelationIdOrderByRecebidoEmAsc(String correlationId);
}
