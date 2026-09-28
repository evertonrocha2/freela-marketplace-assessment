package br.com.freela.common.idempotency;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EventoProcessadoRepository extends JpaRepository<EventoProcessado, String> {

    List<EventoProcessado> findByContratoIdOrderByProcessadoEmAsc(UUID contratoId);

    /** Marcas de um contrato, ou todas quando {@code contratoId} for nulo. */
    default List<EventoProcessadoResponse> listar(UUID contratoId) {
        List<EventoProcessado> eventos = contratoId == null ? findAll() : findByContratoIdOrderByProcessadoEmAsc(contratoId);
        return eventos.stream().map(EventoProcessadoResponse::de).toList();
    }
}
