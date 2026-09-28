package br.com.freela.contrato.infrastructure.outbox;

import br.com.freela.common.events.ContratoEventoPayload;
import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.events.KafkaTopicos;
import br.com.freela.common.json.EventoJson;
import br.com.freela.contrato.application.port.RegistroDeEventos;
import br.com.freela.contrato.domain.event.ContratoEvento;
import br.com.freela.contrato.domain.event.SnapshotContrato;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.tracing.ContextoTrace;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Grava os eventos de dominio na outbox.
 *
 * <p>{@code Propagation.MANDATORY} e proposital: este metodo so pode rodar dentro da transacao que
 * esta salvando o agregado. E isso que da a atomicidade exigida pelo padrao outbox. Se alguem
 * chamar fora de transacao, o Spring falha na hora em vez de deixar passar uma escrita que poderia
 * ser commitada sozinha.</p>
 */
@Component
public class RegistroDeEventosOutbox implements RegistroDeEventos {

    private static final Logger log = LoggerFactory.getLogger(RegistroDeEventosOutbox.class);
    private static final String AGGREGATE_TYPE = "Contrato";
    /** Vai no campo {@code producer} do envelope e no header de mesmo nome. */
    static final String PRODUTOR = "contrato-service";

    private final MensagemOutboxRepository repository;
    private final EventoJson json;
    private final ContextoTrace contextoTrace;

    public RegistroDeEventosOutbox(MensagemOutboxRepository repository, EventoJson json, ContextoTrace contextoTrace) {
        this.repository = repository;
        this.json = json;
        this.contextoTrace = contextoTrace;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void registrar(List<DomainEvent> eventos, String correlationId) {
        String traceparent = contextoTrace.traceparentAtual();
        for (DomainEvent evento : eventos) {
            MensagemOutbox mensagem = montar(evento, correlationId, traceparent);
            repository.save(mensagem);
            log.info("contrato.outbox.registrado eventId={} eventType={} contratoId={} topico={} chave={}",
                    mensagem.getEventId(), mensagem.getEventType(), mensagem.getContratoId(),
                    mensagem.getTopico(), mensagem.getChave());
        }
    }

    private MensagemOutbox montar(DomainEvent evento, String correlationId, String traceparent) {
        if (!(evento instanceof ContratoEvento contratoEvento)) {
            throw new IllegalArgumentException("Evento de dominio nao suportado: " + evento.getClass().getName());
        }
        SnapshotContrato snapshot = contratoEvento.contrato();
        ContratoEventoPayload payload = new ContratoEventoPayload(
                snapshot.contratoId(),
                snapshot.clienteId(),
                snapshot.freelancerId(),
                snapshot.titulo(),
                snapshot.valor(),
                snapshot.status().name(),
                contratoEvento.motivo());

        EventoEnvelope envelope = new EventoEnvelope(
                evento.eventId(),
                evento.eventType(),
                EventoEnvelope.VERSAO_ATUAL,
                AGGREGATE_TYPE,
                evento.aggregateId(),
                snapshot.contratoId(),
                evento.occurredAt(),
                correlationId,
                null,
                PRODUTOR,
                json.paraArvore(payload));

        return new MensagemOutbox(
                envelope.eventId(),
                envelope.eventType(),
                envelope.eventVersion(),
                envelope.aggregateType(),
                envelope.aggregateId(),
                envelope.contratoId(),
                KafkaTopicos.CONTRATOS_EVENTOS,
                envelope.chaveParticionamento(),
                json.escrever(envelope),
                correlationId,
                traceparent,
                envelope.occurredAt());
    }
}
