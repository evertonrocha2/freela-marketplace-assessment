package br.com.freela.contrato.domain.shared;

import java.time.Instant;
import java.util.UUID;

public interface DomainEvent {

    UUID eventId();

    Instant occurredAt();

    String eventType();

    /** Identificador do agregado que originou o evento. Vira a chave da mensagem no Kafka. */
    UUID aggregateId();
}
