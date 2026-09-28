package br.com.freela.auditoria;

import br.com.freela.common.events.EventoHeaders;
import br.com.freela.common.events.KafkaTopicos;
import br.com.freela.common.kafka.HeadersKafka;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Reenvia uma mensagem do DLT para o topico principal.
 *
 * <p>Os consumidores que ja tinham processado o evento antes da falha o descartam pela
 * idempotencia; o que falhou tenta de novo. E por isso que reprocessar e uma operacao segura
 * aqui, e nao algo que precise de intervencao manual em cada servico.</p>
 */
@Service
public class ReprocessadorDeFalhas {

    private static final String PRODUTOR = "auditoria-service/reprocessamento";

    private final EventoFalhaRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public ReprocessadorDeFalhas(EventoFalhaRepository repository, KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public EventoFalha reprocessar(UUID id) {
        EventoFalha falha = repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Falha não encontrada: " + id));

        ProducerRecord<String, String> registro = new ProducerRecord<>(
                KafkaTopicos.CONTRATOS_EVENTOS, null, falha.getChave(), falha.getPayload());
        Headers headers = registro.headers();
        HeadersKafka.escrever(headers, EventoHeaders.EVENT_ID, falha.getEventId());
        HeadersKafka.escrever(headers, EventoHeaders.EVENT_TYPE, falha.getEventType());
        HeadersKafka.escrever(headers, EventoHeaders.CONTRATO_ID, falha.getContratoId());
        HeadersKafka.escrever(headers, EventoHeaders.CORRELATION_ID, falha.getCorrelationId());
        HeadersKafka.escrever(headers, EventoHeaders.PRODUCER, PRODUTOR);
        kafkaTemplate.send(registro);

        falha.marcarReenviado();
        repository.save(falha);
        return falha;
    }
}
