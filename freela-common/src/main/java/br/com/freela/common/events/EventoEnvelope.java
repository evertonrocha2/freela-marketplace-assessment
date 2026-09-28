package br.com.freela.common.events;

import java.time.Instant;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

/**
 * Envelope comum de todas as mensagens trafegadas no Kafka.
 *
 * <p>Separar envelope (metadados) de payload (dados de negocio) permite que consumidores genericos
 * como a auditoria tratem qualquer evento sem conhecer o payload, enquanto consumidores de negocio
 * desserializam o payload no tipo que esperam.</p>
 *
 * @param eventId       identificador unico da mensagem; base da idempotencia dos consumidores
 * @param eventType     tipo do evento (ver {@link EventoTipos})
 * @param eventVersion  versao do schema deste tipo de evento
 * @param aggregateType nome do agregado de origem (sempre {@code Contrato} hoje)
 * @param aggregateId   identificador do agregado; igual a {@code contratoId}
 * @param contratoId    contrato relacionado; tambem e a chave de particionamento no Kafka
 * @param occurredAt    momento em que o fato ocorreu no dominio (nao o momento da publicacao)
 * @param correlationId identificador da operacao originada no API Gateway
 * @param causationId   identificador do evento que causou este; nulo quando a origem foi uma request HTTP
 * @param producer      servico que publicou a mensagem
 * @param payload       dados de negocio (ver {@link ContratoEventoPayload})
 */
public record EventoEnvelope(
        UUID eventId,
        String eventType,
        int eventVersion,
        String aggregateType,
        UUID aggregateId,
        UUID contratoId,
        Instant occurredAt,
        String correlationId,
        String causationId,
        String producer,
        JsonNode payload) {

    public static final int VERSAO_ATUAL = 1;

    public EventoEnvelope {
        if (eventId == null) {
            throw new IllegalArgumentException("eventId e obrigatorio");
        }
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("eventType e obrigatorio");
        }
        if (contratoId == null) {
            throw new IllegalArgumentException("contratoId e obrigatorio");
        }
        if (occurredAt == null) {
            throw new IllegalArgumentException("occurredAt e obrigatorio");
        }
    }

    /** Chave de particionamento: garante ordem por contrato. */
    public String chaveParticionamento() {
        return contratoId.toString();
    }
}
