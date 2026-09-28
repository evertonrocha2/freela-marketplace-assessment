package br.com.freela.reputacao;

import br.com.freela.common.idempotency.EventoProcessado;
import br.com.freela.common.idempotency.EventoProcessadoRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/reputacoes")
public class ReputacaoController {

    private static final Logger log = LoggerFactory.getLogger(ReputacaoController.class);

    private final ReputacaoRepository repository;
    private final EventoProcessadoRepository eventosProcessados;

    public ReputacaoController(ReputacaoRepository repository, EventoProcessadoRepository eventosProcessados) {
        this.repository = repository;
        this.eventosProcessados = eventosProcessados;
    }

    @GetMapping
    public List<ReputacaoResponse> listar() {
        log.info("http.reputacao.listar");
        return repository.findAll().stream().map(ReputacaoResponse::de).toList();
    }

    @GetMapping("/eventos-processados")
    public List<EventoProcessadoResponse> eventosProcessados(@RequestParam(required = false) UUID contratoId) {
        log.info("http.reputacao.eventos-processados contratoId={}", contratoId);
        List<EventoProcessado> eventos = contratoId == null
                ? eventosProcessados.findAll()
                : eventosProcessados.findByContratoIdOrderByProcessadoEmAsc(contratoId);
        return eventos.stream().map(EventoProcessadoResponse::de).toList();
    }

    @GetMapping("/{freelancerId}")
    public ResponseEntity<ReputacaoResponse> buscar(@PathVariable UUID freelancerId) {
        log.info("http.reputacao.buscar freelancerId={}", freelancerId);
        return repository.findById(freelancerId)
                .map(ReputacaoResponse::de)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    public record ReputacaoResponse(UUID freelancerId, int contratosConcluidos, int contratosCancelados,
                                    BigDecimal valorTotal, UUID ultimoEventoId, Instant atualizadoEm) {

        static ReputacaoResponse de(ReputacaoFreelancer r) {
            return new ReputacaoResponse(r.getFreelancerId(), r.getContratosConcluidos(), r.getContratosCancelados(),
                    r.getValorTotal(), r.getUltimoEventoId(), r.getAtualizadoEm());
        }
    }

    public record EventoProcessadoResponse(UUID eventId, String consumidor, String eventType, UUID contratoId,
                                           String correlationId, Instant processadoEm) {

        static EventoProcessadoResponse de(EventoProcessado e) {
            return new EventoProcessadoResponse(e.getEventId(), e.getConsumidor(), e.getEventType(),
                    e.getContratoId(), e.getCorrelationId(), e.getProcessadoEm());
        }
    }
}
