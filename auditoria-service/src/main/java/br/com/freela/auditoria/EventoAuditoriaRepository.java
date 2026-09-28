package br.com.freela.auditoria;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EventoAuditoriaRepository extends JpaRepository<EventoAuditoria, UUID> {

    List<EventoAuditoria> findByContratoIdOrderByOccurredAtAsc(UUID contratoId);

    List<EventoAuditoria> findByCorrelationIdOrderByOccurredAtAsc(String correlationId);

    List<EventoAuditoria> findByEventTypeOrderByOccurredAtAsc(String eventType);

    List<EventoAuditoria> findByEventId(UUID eventId);
}
