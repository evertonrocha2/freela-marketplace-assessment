package br.com.freela.common.kafka;

import br.com.freela.common.correlation.EscopoMdc;
import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.json.EventoJson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base dos consumidores de evento.
 *
 * <p>Centraliza o que precisa ser igual nos tres servicos: contexto de log, desserializacao do
 * envelope, marcacao de inicio e fim do processamento e registro de falha. Cada servico so
 * implementa {@link #processar(EventoEnvelope)}, que roda dentro da sua propria transacao.</p>
 *
 * <p>Nao ha captura de excecao aqui de proposito. A falha precisa subir ate o container para que a
 * politica de retentativa e o envio ao DLT entrem em acao; engolir o erro faria o offset avancar e
 * a mensagem sumir.</p>
 */
public abstract class ConsumidorDeEventos {

    private static final Logger log = LoggerFactory.getLogger(ConsumidorDeEventos.class);

    protected final EventoJson json;
    private final String servico;

    protected ConsumidorDeEventos(EventoJson json, String servico) {
        this.json = json;
        this.servico = servico;
    }

    public void consumir(ConsumerRecord<String, String> registro) {
        try (EscopoMdc escopo = EscopoMdc.de(HeadersKafka.mdc(registro))) {
            long inicio = System.nanoTime();
            log.info("kafka.consumo.inicio servico={} topico={} particao={} offset={} chave={} thread={}",
                    servico, registro.topic(), registro.partition(), registro.offset(), registro.key(),
                    Thread.currentThread().getName());
            try {
                EventoEnvelope envelope = json.lerEnvelope(registro.value());
                ResultadoConsumo resultado = processar(envelope);
                log.info("kafka.consumo.fim servico={} eventType={} contratoId={} resultado={} duracaoMs={}",
                        servico, envelope.eventType(), envelope.contratoId(), resultado,
                        (System.nanoTime() - inicio) / 1_000_000);
            } catch (RuntimeException e) {
                log.error("kafka.consumo.falha servico={} topico={} particao={} offset={} erro={}",
                        servico, registro.topic(), registro.partition(), registro.offset(), e.getMessage(), e);
                throw e;
            }
        }
    }

    protected abstract ResultadoConsumo processar(EventoEnvelope envelope);
}
