package br.com.freela.auditoria;

import br.com.freela.common.correlation.EscopoMdc;
import br.com.freela.common.events.EventoHeaders;
import br.com.freela.common.events.KafkaTopicos;
import br.com.freela.common.kafka.HeadersKafka;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Le o dead letter topic e guarda as mensagens que nenhum consumidor conseguiu processar.
 *
 * <p>A mensagem nao e retirada do Kafka: o DLT continua sendo a fonte. Este registro existe para
 * poder consultar a falha por contrato ou correlationId e para permitir o reenvio pela API, que e
 * o que atende o requisito de manter a mensagem disponivel para diagnostico e reprocessamento.</p>
 *
 * <p>Concorrencia 1 de proposito: o volume do DLT e baixo e o processamento em ordem facilita a
 * leitura do historico de falhas.</p>
 *
 * <p>Cada falha e identificada pela posicao de onde a mensagem saiu (topico, particao e offset
 * originais) e pelo grupo consumidor que falhou. O grupo entra na chave porque a mesma mensagem e
 * lida pelos tres servicos: se ela for invalida, cada um manda a sua copia ao DLT, e essas sao tres
 * falhas distintas. Ja a mesma copia entregue de novo (rebalanceamento, reinicio antes do commit
 * do offset) encontra o registro existente e nao e duplicada. Uma mensagem reprocessada que falhar
 * outra vez chega com offset original novo, entao vira uma falha nova, como deve ser.</p>
 */
@Component
public class DltListener {

    private static final Logger log = LoggerFactory.getLogger(DltListener.class);

    private final EventoFalhaRepository repository;

    public DltListener(EventoFalhaRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(
            id = "auditoria-dlt",
            topics = KafkaTopicos.CONTRATOS_EVENTOS_DLT,
            groupId = "${freela.kafka.grupo-consumidor}-dlt",
            concurrency = "1",
            containerFactory = DltConsumidorConfig.FACTORY)
    @Transactional
    public void onMensagemComErro(ConsumerRecord<String, String> registro) {
        try (EscopoMdc escopo = HeadersKafka.escopoDeLog(registro)) {
            Headers headers = registro.headers();
            String topicoOriginal = HeadersKafka.ler(headers, KafkaHeaders.DLT_ORIGINAL_TOPIC);
            Integer particaoOriginal = HeadersKafka.lerInt(headers, KafkaHeaders.DLT_ORIGINAL_PARTITION);
            Long offsetOriginal = HeadersKafka.lerLong(headers, KafkaHeaders.DLT_ORIGINAL_OFFSET);
            String grupoConsumidor = HeadersKafka.ler(headers, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP);
            if (jaRegistrada(topicoOriginal, particaoOriginal, offsetOriginal, grupoConsumidor)) {
                log.info("dlt.mensagem.duplicada topicoOriginal={} particaoOriginal={} offsetOriginal={} grupoConsumidor={} acao=ignorada",
                        topicoOriginal, particaoOriginal, offsetOriginal, grupoConsumidor);
                return;
            }

            EventoFalha falha = new EventoFalha(
                    paraUuid(HeadersKafka.ler(headers, EventoHeaders.EVENT_ID)),
                    HeadersKafka.ler(headers, EventoHeaders.EVENT_TYPE),
                    paraUuid(HeadersKafka.ler(headers, EventoHeaders.CONTRATO_ID)),
                    HeadersKafka.ler(headers, EventoHeaders.CORRELATION_ID),
                    topicoOriginal,
                    particaoOriginal,
                    offsetOriginal,
                    grupoConsumidor,
                    HeadersKafka.ler(headers, KafkaHeaders.DLT_EXCEPTION_FQCN),
                    HeadersKafka.ler(headers, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                    registro.key(),
                    registro.value());
            repository.save(falha);

            log.error("dlt.mensagem.registrada falhaId={} eventId={} contratoId={} grupoConsumidor={} topicoOriginal={} particaoOriginal={} offsetOriginal={} excecao={}",
                    falha.getId(), falha.getEventId(), falha.getContratoId(), falha.getGrupoConsumidor(),
                    falha.getTopicoOriginal(), falha.getParticaoOriginal(), falha.getOffsetOriginal(), falha.getExcecao());
        }
    }

    /** A mesma copia entregue de novo. Sem os headers de origem nao ha como reconhecer, e ela e registrada. */
    private boolean jaRegistrada(String topico, Integer particao, Long offset, String grupo) {
        return topico != null && particao != null && offset != null && grupo != null
                && repository.existsByTopicoOriginalAndParticaoOriginalAndOffsetOriginalAndGrupoConsumidor(
                        topico, particao, offset, grupo);
    }

    private static UUID paraUuid(String valor) {
        try {
            return valor == null ? null : UUID.fromString(valor);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
