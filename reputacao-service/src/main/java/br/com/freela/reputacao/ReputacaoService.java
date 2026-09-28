package br.com.freela.reputacao;

import br.com.freela.common.events.ContratoEventoPayload;
import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.events.EventoTipos;
import br.com.freela.common.idempotency.ControleIdempotencia;
import br.com.freela.common.json.EventoJson;
import br.com.freela.common.kafka.ResultadoConsumo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Mantem os numeros agregados de cada freelancer.
 *
 * <p>E o consumidor onde duplicidade doeria mais: um contador incrementado duas vezes nao tem como
 * ser distinguido de dois contratos reais depois. Por isso o incremento so acontece depois que o
 * {@code eventId} foi aceito como inedito, e ambas as escritas vao na mesma transacao.</p>
 */
@Service
public class ReputacaoService {

    private static final Logger log = LoggerFactory.getLogger(ReputacaoService.class);
    private static final String CONSUMIDOR = "reputacao-service";

    private final ReputacaoRepository repository;
    private final ControleIdempotencia idempotencia;
    private final EventoJson json;

    public ReputacaoService(ReputacaoRepository repository, ControleIdempotencia idempotencia, EventoJson json) {
        this.repository = repository;
        this.idempotencia = idempotencia;
        this.json = json;
    }

    @Transactional
    public ResultadoConsumo processar(EventoEnvelope envelope) {
        if (!interessa(envelope.eventType())) {
            log.info("reputacao.evento.ignorado eventType={} contratoId={} motivo=sem-impacto-na-reputacao",
                    envelope.eventType(), envelope.contratoId());
            return ResultadoConsumo.IGNORADO_POR_TIPO;
        }
        if (!idempotencia.registrarSeInedito(envelope, CONSUMIDOR)) {
            log.info("reputacao.evento.duplicado eventId={} contratoId={} contadorInalterado=true",
                    envelope.eventId(), envelope.contratoId());
            return ResultadoConsumo.DUPLICADO_IGNORADO;
        }

        ContratoEventoPayload payload = json.lerPayload(envelope.payload(), ContratoEventoPayload.class);
        log.info("reputacao.atualizacao.inicio eventId={} eventType={} contratoId={} freelancerId={} valor={}",
                envelope.eventId(), envelope.eventType(), envelope.contratoId(),
                payload.freelancerId(), payload.valor());

        ReputacaoFreelancer reputacao = repository.findById(payload.freelancerId())
                .orElseGet(() -> new ReputacaoFreelancer(payload.freelancerId()));

        if (EventoTipos.CONTRATO_CONCLUIDO.equals(envelope.eventType())) {
            reputacao.registrarConclusao(payload.valor(), envelope.eventId());
        } else {
            reputacao.registrarCancelamento(envelope.eventId());
        }
        repository.save(reputacao);

        log.info("reputacao.atualizacao.sucesso eventId={} contratoId={} freelancerId={} concluidos={} cancelados={} valorTotal={}",
                envelope.eventId(), envelope.contratoId(), reputacao.getFreelancerId(),
                reputacao.getContratosConcluidos(), reputacao.getContratosCancelados(), reputacao.getValorTotal());
        return ResultadoConsumo.APLICADO;
    }

    private boolean interessa(String eventType) {
        return EventoTipos.CONTRATO_CONCLUIDO.equals(eventType) || EventoTipos.CONTRATO_CANCELADO.equals(eventType);
    }
}
