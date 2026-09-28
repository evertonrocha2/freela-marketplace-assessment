package br.com.freela.common.kafka;

import br.com.freela.common.events.EventoHeaders;
import br.com.freela.common.events.KafkaTopicos;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.ExponentialBackOff;
import tools.jackson.core.JacksonException;

/**
 * Politica de falha dos consumidores.
 *
 * <p>Retentativa e <b>bloqueante</b>, feita pelo proprio container: a particao fica pausada enquanto
 * o registro e retentado. Essa escolha e deliberada. O alternativo (topicos de retry via
 * {@code @RetryableTopic}) devolveria a mensagem para o fim de outra fila e quebraria a ordem dos
 * eventos do contrato, que e um requisito do sistema. O preco e que uma mensagem problematica
 * segura a particao dela por alguns segundos; as outras duas particoes seguem normalmente, entao
 * contratos nao relacionados nao param.</p>
 *
 * <p>Esgotadas as tentativas, a mensagem vai para o DLT <b>na mesma particao de origem</b>, junto
 * com headers de diagnostico gravados pelo Spring Kafka (topico, particao, offset, excecao e
 * stacktrace originais). Ela fica la disponivel para inspecao e reprocessamento.</p>
 *
 * <p>Erros de desserializacao e de JSON nao sao retentados: tentar de novo produziria exatamente o
 * mesmo erro. Vao direto para o DLT.</p>
 */
@Configuration
@ConditionalOnProperty(name = "freela.kafka.consumidor.habilitado", havingValue = "true", matchIfMissing = true)
public class KafkaConsumidorConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumidorConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaOperations<Object, Object> kafkaOperations) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaOperations,
                (registro, excecao) -> {
                    log.error("kafka.consumo.dlt topicoOrigem={} particao={} offset={} eventId={} contratoId={} erro={}",
                            registro.topic(), registro.partition(), registro.offset(),
                            header(registro.headers(), EventoHeaders.EVENT_ID),
                            header(registro.headers(), EventoHeaders.CONTRATO_ID),
                            excecao.getMessage(), excecao);
                    return new TopicPartition(KafkaTopicos.CONTRATOS_EVENTOS_DLT, registro.partition());
                });

        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxAttempts(3);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(
                DeserializationException.class,
                JacksonException.class,
                IllegalArgumentException.class);
        handler.setRetryListeners((registro, excecao, tentativa) ->
                log.warn("kafka.consumo.retentativa tentativa={} topico={} particao={} offset={} eventId={} erro={}",
                        tentativa, registro.topic(), registro.partition(), registro.offset(),
                        header(registro.headers(), EventoHeaders.EVENT_ID), excecao.getMessage()));
        return handler;
    }

    private static String header(org.apache.kafka.common.header.Headers headers, String nome) {
        var header = headers.lastHeader(nome);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
