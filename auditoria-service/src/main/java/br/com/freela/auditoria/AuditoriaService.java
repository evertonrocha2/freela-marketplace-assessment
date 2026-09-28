package br.com.freela.auditoria;

import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.idempotency.ControleIdempotencia;
import br.com.freela.common.json.EventoJson;
import br.com.freela.common.kafka.ResultadoConsumo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Registra todos os eventos que passam pela plataforma.
 *
 * <p>Ao contrario dos outros consumidores, a auditoria nao filtra por tipo: qualquer evento que
 * apareca no topico, inclusive tipos criados depois, e gravado. O envelope traz metadados
 * suficientes para isso sem que a auditoria precise conhecer o payload.</p>
 */
@Service
public class AuditoriaService {

    private static final Logger log = LoggerFactory.getLogger(AuditoriaService.class);
    private static final String CONSUMIDOR = "auditoria-service";

    private final EventoAuditoriaRepository repository;
    private final ControleIdempotencia idempotencia;
    private final EventoJson json;

    public AuditoriaService(EventoAuditoriaRepository repository, ControleIdempotencia idempotencia, EventoJson json) {
        this.repository = repository;
        this.idempotencia = idempotencia;
        this.json = json;
    }

    @Transactional
    public ResultadoConsumo processar(EventoEnvelope envelope) {
        if (!idempotencia.registrarSeInedito(envelope, CONSUMIDOR)) {
            log.info("auditoria.evento.duplicado eventId={} contratoId={} nenhumRegistroCriado=true",
                    envelope.eventId(), envelope.contratoId());
            return ResultadoConsumo.DUPLICADO_IGNORADO;
        }

        log.info("auditoria.registro.inicio eventId={} eventType={} contratoId={} correlationId={} occurredAt={}",
                envelope.eventId(), envelope.eventType(), envelope.contratoId(),
                envelope.correlationId(), envelope.occurredAt());

        EventoAuditoria registro = new EventoAuditoria(
                envelope.eventId(),
                envelope.eventType(),
                envelope.eventVersion(),
                envelope.aggregateType(),
                envelope.aggregateId(),
                envelope.contratoId(),
                envelope.correlationId(),
                envelope.producer(),
                envelope.occurredAt(),
                json.escrever(envelope.payload()));
        repository.save(registro);

        log.info("auditoria.registro.sucesso auditoriaId={} eventId={} eventType={} contratoId={}",
                registro.getId(), envelope.eventId(), envelope.eventType(), envelope.contratoId());
        return ResultadoConsumo.APLICADO;
    }
}
