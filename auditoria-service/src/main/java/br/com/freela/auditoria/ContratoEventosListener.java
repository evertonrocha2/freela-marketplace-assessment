package br.com.freela.auditoria;

import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.events.KafkaTopicos;
import br.com.freela.common.json.EventoJson;
import br.com.freela.common.kafka.ConsumidorDeEventos;
import br.com.freela.common.kafka.ResultadoConsumo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ContratoEventosListener extends ConsumidorDeEventos {

    private final AuditoriaService service;

    public ContratoEventosListener(EventoJson json, AuditoriaService service) {
        super(json, AuditoriaService.CONSUMIDOR);
        this.service = service;
    }

    @KafkaListener(
            id = "auditoria-contratos",
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
