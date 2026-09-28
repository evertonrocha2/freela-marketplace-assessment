package br.com.freela.notificacao;

import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.events.KafkaTopicos;
import br.com.freela.common.json.EventoJson;
import br.com.freela.common.kafka.ConsumidorDeEventos;
import br.com.freela.common.kafka.ResultadoConsumo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor dos eventos de contrato.
 *
 * <p>{@code concurrency} cria uma thread por particao. Com tres particoes, tres contratos
 * diferentes sao processados ao mesmo tempo; dois eventos do mesmo contrato caem sempre na mesma
 * particao (a chave e o contratoId) e portanto sao entregues em sequencia a mesma thread. Subir a
 * concorrencia acima do numero de particoes so criaria consumidores ociosos.</p>
 *
 * <p>O processamento transacional fica em {@link NotificacaoService} e nao aqui: uma chamada de um
 * metodo {@code @Transactional} do proprio objeto nao passaria pelo proxy do Spring e a transacao
 * simplesmente nao abriria.</p>
 */
@Component
public class ContratoEventosListener extends ConsumidorDeEventos {

    private final NotificacaoService service;

    public ContratoEventosListener(EventoJson json, NotificacaoService service) {
        super(json, NotificacaoService.CONSUMIDOR);
        this.service = service;
    }

    @KafkaListener(
            id = "notificacao-contratos",
            topics = KafkaTopicos.CONTRATOS_EVENTOS,
            groupId = "${freela.kafka.grupo-consumidor}",
            concurrency = "${freela.kafka.concorrencia}")
    public void onEventoDeContrato(ConsumerRecord<String, String> registro) {
        consumir(registro);
    }

    @Override
    protected ResultadoConsumo processar(EventoEnvelope envelope) {
        return service.processar(envelope);
    }
}
