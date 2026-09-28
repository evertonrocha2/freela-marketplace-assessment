package br.com.freela.reputacao;

import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.events.KafkaTopicos;
import br.com.freela.common.json.EventoJson;
import br.com.freela.common.kafka.ConsumidorDeEventos;
import br.com.freela.common.kafka.ResultadoConsumo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consome o mesmo topico do notificacao-service, em um grupo proprio.
 *
 * <p>Grupos distintos e o que faz cada servico receber uma copia de todos os eventos e manter o
 * proprio offset: um consumidor lento ou parado nao atrasa nem perde mensagens para os outros.</p>
 */
@Component
public class ContratoEventosListener extends ConsumidorDeEventos {

    private final ReputacaoService service;

    public ContratoEventosListener(EventoJson json, ReputacaoService service) {
        super(json, ReputacaoService.CONSUMIDOR);
        this.service = service;
    }

    @KafkaListener(
            id = "reputacao-contratos",
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
