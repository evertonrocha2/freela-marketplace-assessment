package br.com.freela.auditoria;

import br.com.freela.common.events.EventoHeaders;
import br.com.freela.common.events.KafkaTopicos;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/auditoria")
public class AuditoriaController {

    private static final Logger log = LoggerFactory.getLogger(AuditoriaController.class);

    private final EventoAuditoriaRepository repository;
    private final EventoFalhaRepository falhaRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public AuditoriaController(EventoAuditoriaRepository repository, EventoFalhaRepository falhaRepository,
                               KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.falhaRepository = falhaRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping
    public List<EventoAuditoriaResponse> listar(@RequestParam(required = false) UUID contratoId,
                                                @RequestParam(required = false) String correlationId,
                                                @RequestParam(required = false) String eventType) {
        log.info("http.auditoria.listar contratoId={} correlationId={} eventType={}",
                contratoId, correlationId, eventType);
        List<EventoAuditoria> eventos;
        if (contratoId != null) {
            eventos = repository.findByContratoIdOrderByOccurredAtAsc(contratoId);
        } else if (correlationId != null) {
            eventos = repository.findByCorrelationIdOrderByOccurredAtAsc(correlationId);
        } else if (eventType != null) {
            eventos = repository.findByEventTypeOrderByOccurredAtAsc(eventType);
        } else {
            eventos = repository.findAll();
        }
        return eventos.stream().map(EventoAuditoriaResponse::de).toList();
    }

    @GetMapping("/falhas")
    public List<EventoFalhaResponse> falhas(@RequestParam(required = false) UUID contratoId,
                                            @RequestParam(required = false) String correlationId) {
        log.info("http.auditoria.falhas contratoId={} correlationId={}", contratoId, correlationId);
        List<EventoFalha> falhas;
        if (contratoId != null) {
            falhas = falhaRepository.findByContratoIdOrderByRecebidoEmAsc(contratoId);
        } else if (correlationId != null) {
            falhas = falhaRepository.findByCorrelationIdOrderByRecebidoEmAsc(correlationId);
        } else {
            falhas = falhaRepository.findAll();
        }
        return falhas.stream().map(EventoFalhaResponse::de).toList();
    }

    /**
     * Reenvia uma mensagem do DLT para o topico principal.
     *
     * <p>Os consumidores que ja tinham processado o evento antes da falha o descartam pela
     * idempotencia; o que falhou tenta de novo. E por isso que reprocessar e uma operacao segura
     * aqui, e nao algo que precise de intervencao manual em cada servico.</p>
     */
    @PostMapping("/falhas/{id}/reprocessar")
    public EventoFalhaResponse reprocessar(@PathVariable UUID id) {
        EventoFalha falha = falhaRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Falha não encontrada: " + id));

        ProducerRecord<String, String> registro = new ProducerRecord<>(
                KafkaTopicos.CONTRATOS_EVENTOS, null, falha.getChave(), falha.getPayload());
        header(registro, EventoHeaders.EVENT_ID, String.valueOf(falha.getEventId()));
        header(registro, EventoHeaders.EVENT_TYPE, falha.getEventType());
        header(registro, EventoHeaders.CONTRATO_ID, String.valueOf(falha.getContratoId()));
        header(registro, EventoHeaders.CORRELATION_ID, falha.getCorrelationId());
        header(registro, EventoHeaders.PRODUCER, "auditoria-service/reprocessamento");
        kafkaTemplate.send(registro);

        falha.marcarReenviado();
        falhaRepository.save(falha);
        log.warn("dlt.mensagem.reprocessada falhaId={} eventId={} contratoId={} topicoDestino={}",
                falha.getId(), falha.getEventId(), falha.getContratoId(), KafkaTopicos.CONTRATOS_EVENTOS);
        return EventoFalhaResponse.de(falha);
    }

    private void header(ProducerRecord<String, String> registro, String nome, String valor) {
        if (valor != null && !"null".equals(valor)) {
            registro.headers().add(nome, valor.getBytes(StandardCharsets.UTF_8));
        }
    }

    public record EventoAuditoriaResponse(UUID id, UUID eventId, String eventType, int eventVersion,
                                          String aggregateType, UUID aggregateId, UUID contratoId,
                                          String correlationId, String producer, Instant occurredAt,
                                          Instant recebidoEm, String payload) {

        static EventoAuditoriaResponse de(EventoAuditoria e) {
            return new EventoAuditoriaResponse(e.getId(), e.getEventId(), e.getEventType(), e.getEventVersion(),
                    e.getAggregateType(), e.getAggregateId(), e.getContratoId(), e.getCorrelationId(),
                    e.getProducer(), e.getOccurredAt(), e.getRecebidoEm(), e.getPayload());
        }
    }

    public record EventoFalhaResponse(UUID id, UUID eventId, String eventType, UUID contratoId, String correlationId,
                                      String topicoOriginal, Integer particaoOriginal, Long offsetOriginal,
                                      String excecao, String mensagemErro, Instant recebidoEm, Instant reenviadoEm,
                                      String payload) {

        static EventoFalhaResponse de(EventoFalha f) {
            return new EventoFalhaResponse(f.getId(), f.getEventId(), f.getEventType(), f.getContratoId(),
                    f.getCorrelationId(), f.getTopicoOriginal(), f.getParticaoOriginal(), f.getOffsetOriginal(),
                    f.getExcecao(), f.getMensagemErro(), f.getRecebidoEm(), f.getReenviadoEm(), f.getPayload());
        }
    }
}
