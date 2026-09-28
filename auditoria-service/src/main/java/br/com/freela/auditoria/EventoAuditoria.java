package br.com.freela.auditoria;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Registro de auditoria de um evento da plataforma.
 *
 * <p>O {@code eventId} tem restricao de unicidade: alem da marca de idempotencia, o proprio schema
 * impede dois registros do mesmo evento.</p>
 */
@Entity
@Table(name = "auditoria_eventos",
        uniqueConstraints = @jakarta.persistence.UniqueConstraint(
                name = "uk_auditoria_event_id", columnNames = "eventId"),
        indexes = {
                @Index(name = "ix_auditoria_contrato", columnList = "contratoId"),
                @Index(name = "ix_auditoria_correlation", columnList = "correlationId"),
                @Index(name = "ix_auditoria_tipo", columnList = "eventType")
        })
public class EventoAuditoria {

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID eventId;

    @Column(nullable = false, length = 60)
    private String eventType;

    private int eventVersion;

    @Column(length = 40)
    private String aggregateType;

    private UUID aggregateId;

    @Column(nullable = false)
    private UUID contratoId;

    @Column(length = 120)
    private String correlationId;

    @Column(length = 60)
    private String producer;

    /** Momento em que o fato ocorreu no dominio. */
    @Column(nullable = false)
    private Instant occurredAt;

    /** Momento em que a auditoria recebeu a mensagem. A diferenca mostra a latencia do pipeline. */
    @Column(nullable = false)
    private Instant recebidoEm;

    @Column(columnDefinition = "text")
    private String payload;

    protected EventoAuditoria() {
    }

    public EventoAuditoria(UUID eventId, String eventType, int eventVersion, String aggregateType, UUID aggregateId,
                           UUID contratoId, String correlationId, String producer, Instant occurredAt, String payload) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.eventType = eventType;
        this.eventVersion = eventVersion;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.contratoId = contratoId;
        this.correlationId = correlationId;
        this.producer = producer;
        this.occurredAt = occurredAt;
        this.payload = payload;
        this.recebidoEm = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public int getEventVersion() {
        return eventVersion;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public UUID getContratoId() {
        return contratoId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getProducer() {
        return producer;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public Instant getRecebidoEm() {
        return recebidoEm;
    }

    public String getPayload() {
        return payload;
    }
}
