package br.com.freela.common.kafka;

import br.com.freela.common.correlation.EscopoMdc;
import br.com.freela.common.correlation.MarcadorDeTrace;
import br.com.freela.common.correlation.MdcKeys;
import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.json.EventoJson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Base dos consumidores de evento.
 *
 * <p>Centraliza o que precisa ser igual nos tres servicos: contexto de log, desserializacao do
 * envelope, marcacao de inicio e fim do processamento e registro de falha. Cada servico so
 * implementa {@link #processar(EventoEnvelope)}, que roda dentro da sua propria transacao.</p>
 *
 * <p>O contexto de log vem primeiro dos headers, antes de ler o corpo: se o corpo estiver
 * corrompido, a linha de erro ainda sai identificada. Depois de ler o envelope, o que faltou nos
 * headers e completado com o que veio no corpo. Isso cobre mensagens publicadas por um produtor
 * que nao preencheu os headers.</p>
 *
 * <p>Nao ha captura de excecao aqui de proposito. A falha precisa subir ate o container para que a
 * politica de retentativa e o envio ao DLT entrem em acao; engolir o erro faria o offset avancar e
 * a mensagem sumir.</p>
 */
public abstract class ConsumidorDeEventos {

    private static final Logger log = LoggerFactory.getLogger(ConsumidorDeEventos.class);

    protected final EventoJson json;
    private final String servico;
    private MarcadorDeTrace marcadorDeTrace;

    protected ConsumidorDeEventos(EventoJson json, String servico) {
        this.json = json;
        this.servico = servico;
    }

    @Autowired
    public void setMarcadorDeTrace(MarcadorDeTrace marcadorDeTrace) {
        this.marcadorDeTrace = marcadorDeTrace;
    }

    public void consumir(ConsumerRecord<String, String> registro) {
        try (EscopoMdc escopo = HeadersKafka.escopoDeLog(registro)) {
            long inicio = System.nanoTime();
            marcarTrace();
            log.info("kafka.consumo.inicio servico={} topico={} particao={} offset={} chave={} thread={}",
                    servico, registro.topic(), registro.partition(), registro.offset(), registro.key(),
                    Thread.currentThread().getName());
            try {
                EventoEnvelope envelope = json.lerEnvelope(registro.value());
                completarContexto(envelope);
                marcarTrace();
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

    /**
     * Preenche no MDC so as chaves que os headers deixaram vazias. As quatro chaves ja foram
     * registradas pelo escopo aberto em {@link #consumir}, entao o fechamento dele restaura tudo.
     */
    private static void completarContexto(EventoEnvelope envelope) {
        EscopoMdc.contextoDeEvento(envelope.correlationId(), envelope.eventId(), envelope.eventType(),
                envelope.contratoId()).forEach(ConsumidorDeEventos::completar);
    }

    private static void completar(String chave, String valor) {
        String atual = MDC.get(chave);
        if (valor != null && (atual == null || atual.isBlank())) {
            MDC.put(chave, valor);
        }
    }

    private void marcarTrace() {
        marcadorDeTrace.marcar(MarcadorDeTrace.TAG_CORRELATION_ID, MDC.get(MdcKeys.CORRELATION_ID));
        marcadorDeTrace.marcar(MarcadorDeTrace.TAG_CONTRATO_ID, MDC.get(MdcKeys.CONTRATO_ID));
    }
}
