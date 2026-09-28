package br.com.freela.reputacao;

import br.com.freela.common.events.ContratoEventoPayload;
import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.events.EventoTipos;
import br.com.freela.common.idempotency.ControleIdempotencia;
import br.com.freela.common.json.EventoJson;
import br.com.freela.common.kafka.ResultadoConsumo;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
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
    private final CriadorDeReputacao criador;
    private final ControleIdempotencia idempotencia;
    private final EventoJson json;

    public ReputacaoService(ReputacaoRepository repository, CriadorDeReputacao criador,
                            ControleIdempotencia idempotencia, EventoJson json) {
        this.repository = repository;
        this.criador = criador;
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

        ReputacaoFreelancer reputacao = travarReputacao(payload.freelancerId());

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

    /**
     * Devolve a reputacao do freelancer ja travada para escrita, criando o registro se preciso.
     *
     * <p>Se outra thread criar o registro no mesmo instante, a insercao daqui falha com violacao de
     * chave. Isso e esperado e nao e erro: basta ler de novo, agora com a linha existindo, e a
     * trava garante que os dois incrementos sejam aplicados um depois do outro.</p>
     */
    private ReputacaoFreelancer travarReputacao(UUID freelancerId) {
        return repository.buscarParaAtualizar(freelancerId).orElseGet(() -> {
            try {
                criador.criarSeAusente(freelancerId);
            } catch (DataIntegrityViolationException corrida) {
                log.info("reputacao.criacao.concorrente freelancerId={} acao=reler-registro-criado-por-outra-thread",
                        freelancerId);
            }
            return repository.buscarParaAtualizar(freelancerId).orElseThrow(() ->
                    new IllegalStateException("reputacao nao encontrada apos criacao: " + freelancerId));
        });
    }

    private boolean interessa(String eventType) {
        return EventoTipos.CONTRATO_CONCLUIDO.equals(eventType) || EventoTipos.CONTRATO_CANCELADO.equals(eventType);
    }
}
