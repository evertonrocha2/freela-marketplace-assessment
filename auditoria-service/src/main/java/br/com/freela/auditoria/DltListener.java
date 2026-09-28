package br.com.freela.auditoria;

import br.com.freela.common.correlation.EscopoMdc;
import br.com.freela.common.events.EventoHeaders;
import br.com.freela.common.events.KafkaTopicos;
import br.com.freela.common.kafka.HeadersKafka;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
            concurrency = "1")
    @Transactional
    public void onMensagemComErro(ConsumerRecord<String, String> registro) {
        try (EscopoMdc escopo = EscopoMdc.de(HeadersKafka.mdc(registro))) {
            String eventIdBruto = HeadersKafka.ler(registro.headers(), EventoHeaders.EVENT_ID);
            String contratoIdBruto = HeadersKafka.ler(registro.headers(), EventoHeaders.CONTRATO_ID);

            EventoFalha falha = new EventoFalha(
                    paraUuid(eventIdBruto),
                    HeadersKafka.ler(registro.headers(), EventoHeaders.EVENT_TYPE),
                    paraUuid(contratoIdBruto),
                    HeadersKafka.ler(registro.headers(), EventoHeaders.CORRELATION_ID),
                    HeadersKafka.ler(registro.headers(), KafkaHeaders.DLT_ORIGINAL_TOPIC),
                    inteiro(registro, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                    longo(registro, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                    HeadersKafka.ler(registro.headers(), KafkaHeaders.DLT_EXCEPTION_FQCN),
                    HeadersKafka.ler(registro.headers(), KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                    registro.key(),
                    registro.value());
            repository.save(falha);

            log.error("dlt.mensagem.registrada falhaId={} eventId={} contratoId={} topicoOriginal={} particaoOriginal={} offsetOriginal={} excecao={}",
                    falha.getId(), falha.getEventId(), falha.getContratoId(), falha.getTopicoOriginal(),
                    falha.getParticaoOriginal(), falha.getOffsetOriginal(), falha.getExcecao());
        }
    }

    private static UUID paraUuid(String valor) {
        try {
            return valor == null ? null : UUID.fromString(valor);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** Spring Kafka grava esses headers como inteiros binarios, nao como texto. */
    private static Integer inteiro(ConsumerRecord<String, String> registro, String nome) {
        var header = registro.headers().lastHeader(nome);
        if (header == null || header.value() == null || header.value().length < Integer.BYTES) {
            return null;
        }
        return ByteBuffer.wrap(header.value()).getInt();
    }

    private static Long longo(ConsumerRecord<String, String> registro, String nome) {
        var header = registro.headers().lastHeader(nome);
        if (header == null || header.value() == null || header.value().length < Long.BYTES) {
            return null;
        }
        return ByteBuffer.wrap(header.value()).getLong();
    }
}
