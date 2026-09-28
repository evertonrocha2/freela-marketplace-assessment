package br.com.freela.notificacao;

import br.com.freela.common.events.ContratoEventoPayload;
import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.events.EventoTipos;
import br.com.freela.common.idempotency.ControleIdempotencia;
import br.com.freela.common.json.EventoJson;
import br.com.freela.common.kafka.ResultadoConsumo;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Registra as notificacoes do ciclo de vida do contrato.
 *
 * <p>A marca de idempotencia e a notificacao sao gravadas na mesma transacao. E isso que faz o
 * reprocessamento da mesma mensagem nao gerar uma segunda notificacao: se a transacao completa, a
 * marca existe e a proxima entrega e descartada; se a transacao falha, nem a marca nem a
 * notificacao ficam no banco e a reentrega funciona normalmente.</p>
 */
@Service
public class NotificacaoService {

    private static final Logger log = LoggerFactory.getLogger(NotificacaoService.class);
    private static final String CONSUMIDOR = "notificacao-service";

    private final NotificacaoRepository repository;
    private final ControleIdempotencia idempotencia;
    private final EventoJson json;

    public NotificacaoService(NotificacaoRepository repository, ControleIdempotencia idempotencia, EventoJson json) {
        this.repository = repository;
        this.idempotencia = idempotencia;
        this.json = json;
    }

    @Transactional
    public ResultadoConsumo processar(EventoEnvelope envelope) {
        if (!interessa(envelope.eventType())) {
            log.info("notificacao.evento.ignorado eventType={} contratoId={}",
                    envelope.eventType(), envelope.contratoId());
            return ResultadoConsumo.IGNORADO_POR_TIPO;
        }
        if (!idempotencia.registrarSeInedito(envelope, CONSUMIDOR)) {
            log.info("notificacao.evento.duplicado eventId={} contratoId={} nenhumaNotificacaoCriada=true",
                    envelope.eventId(), envelope.contratoId());
            return ResultadoConsumo.DUPLICADO_IGNORADO;
        }

        ContratoEventoPayload payload = json.lerPayload(envelope.payload(), ContratoEventoPayload.class);
        UUID destinatario = destinatarioDe(envelope.eventType(), payload);

        log.info("notificacao.registro.inicio eventId={} eventType={} contratoId={} destinatarioId={}",
                envelope.eventId(), envelope.eventType(), envelope.contratoId(), destinatario);

        Notificacao notificacao = new Notificacao(
                envelope.contratoId(),
                destinatario,
                envelope.eventType(),
                mensagemDe(envelope.eventType(), payload),
                envelope.eventId(),
                envelope.correlationId());
        repository.save(notificacao);

        log.info("notificacao.registro.sucesso notificacaoId={} eventId={} contratoId={} destinatarioId={} resultado=CRIADA",
                notificacao.getId(), envelope.eventId(), envelope.contratoId(), destinatario);
        return ResultadoConsumo.APLICADO;
    }

    private boolean interessa(String eventType) {
        return EventoTipos.CONTRATO_CRIADO.equals(eventType)
                || EventoTipos.ENTREGA_REGISTRADA.equals(eventType)
                || EventoTipos.CONTRATO_CONCLUIDO.equals(eventType)
                || EventoTipos.CONTRATO_CANCELADO.equals(eventType);
    }

    /** Quem precisa saber do que aconteceu: a entrega avisa o cliente, o resto avisa o freelancer. */
    private UUID destinatarioDe(String eventType, ContratoEventoPayload payload) {
        return EventoTipos.ENTREGA_REGISTRADA.equals(eventType) ? payload.clienteId() : payload.freelancerId();
    }

    private String mensagemDe(String eventType, ContratoEventoPayload payload) {
        return switch (eventType) {
            case EventoTipos.CONTRATO_CRIADO ->
                    "Novo contrato: " + payload.titulo();
            case EventoTipos.ENTREGA_REGISTRADA ->
                    "Entrega registrada no contrato: " + payload.titulo();
            case EventoTipos.CONTRATO_CONCLUIDO ->
                    "Contrato concluído: " + payload.titulo();
            case EventoTipos.CONTRATO_CANCELADO ->
                    "Contrato cancelado: " + payload.titulo()
                            + (payload.motivo() == null ? "" : " (" + payload.motivo() + ")");
            default -> "Atualização do contrato: " + payload.titulo();
        };
    }
}
