package br.com.freela.notificacao;

import br.com.freela.common.idempotency.EventoProcessado;
import br.com.freela.common.idempotency.EventoProcessadoRepository;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/notificacoes")
public class NotificacaoController {

    private static final Logger log = LoggerFactory.getLogger(NotificacaoController.class);

    private final NotificacaoRepository repository;
    private final EventoProcessadoRepository eventosProcessados;

    public NotificacaoController(NotificacaoRepository repository, EventoProcessadoRepository eventosProcessados) {
        this.repository = repository;
        this.eventosProcessados = eventosProcessados;
    }

    @GetMapping
    public List<NotificacaoResponse> listar(@RequestParam(required = false) UUID contratoId,
                                            @RequestParam(required = false) String correlationId) {
        log.info("http.notificacao.listar contratoId={} correlationId={}", contratoId, correlationId);
        List<Notificacao> notificacoes;
        if (contratoId != null) {
            notificacoes = repository.findByContratoIdOrderByCriadaEmAsc(contratoId);
        } else if (correlationId != null) {
            notificacoes = repository.findByCorrelationIdOrderByCriadaEmAsc(correlationId);
        } else {
            notificacoes = repository.findAll();
        }
        return notificacoes.stream().map(NotificacaoResponse::de).toList();
    }

    /** Evidencia da idempotencia: mostra quais eventIds ja foram aplicados por este servico. */
    @GetMapping("/eventos-processados")
    public List<EventoProcessadoResponse> eventosProcessados(@RequestParam(required = false) UUID contratoId) {
        log.info("http.notificacao.eventos-processados contratoId={}", contratoId);
        List<EventoProcessado> eventos = contratoId == null
                ? eventosProcessados.findAll()
                : eventosProcessados.findByContratoIdOrderByProcessadoEmAsc(contratoId);
        return eventos.stream().map(EventoProcessadoResponse::de).toList();
    }

    public record NotificacaoResponse(UUID id, UUID contratoId, UUID destinatarioId, String tipo, String mensagem,
                                      UUID eventId, String correlationId, Instant criadaEm) {

        static NotificacaoResponse de(Notificacao n) {
            return new NotificacaoResponse(n.getId(), n.getContratoId(), n.getDestinatarioId(), n.getTipo(),
                    n.getMensagem(), n.getEventId(), n.getCorrelationId(), n.getCriadaEm());
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
