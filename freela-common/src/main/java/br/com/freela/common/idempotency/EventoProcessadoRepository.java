package br.com.freela.common.idempotency;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EventoProcessadoRepository extends JpaRepository<EventoProcessado, String> {

    List<EventoProcessado> findByContratoIdOrderByProcessadoEmAsc(UUID contratoId);

    List<EventoProcessado> findByEventId(UUID eventId);
}
