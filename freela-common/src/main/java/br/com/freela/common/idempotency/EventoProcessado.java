package br.com.freela.common.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Registro de que um eventId ja foi processado por um consumidor.
 *
 * <p>Vive no banco do proprio servico e e gravado na mesma transacao do efeito de negocio.
 * Se a transacao falhar, a marca tambem some e a mensagem pode ser reprocessada sem perda.</p>
 *
 * <p>A chave e {@code eventId:consumidor} para que um mesmo servico possa ter mais de um consumidor
 * logico sobre o mesmo topico no futuro sem que um bloqueie o outro.</p>
 */
@Entity
@Table(name = "eventos_processados",
        indexes = {
                @Index(name = "ix_eventos_processados_event_id", columnList = "eventId"),
                @Index(name = "ix_eventos_processados_contrato", columnList = "contratoId")
        })
public class EventoProcessado {

    @Id
    @Column(length = 120)
    private String id;

    @Column(nullable = false)
    private UUID eventId;

    @Column(nullable = false, length = 60)
    private String consumidor;

    @Column(length = 60)
    private String eventType;

    private UUID contratoId;

    @Column(length = 120)
    private String correlationId;

    @Column(nullable = false)
    private Instant processadoEm;

    protected EventoProcessado() {
    }

    public EventoProcessado(UUID eventId, String consumidor, String eventType, UUID contratoId, String correlationId) {
        this.id = chave(eventId, consumidor);
        this.eventId = eventId;
        this.consumidor = consumidor;
        this.eventType = eventType;
        this.contratoId = contratoId;
        this.correlationId = correlationId;
        this.processadoEm = Instant.now();
    }

    public static String chave(UUID eventId, String consumidor) {
        return eventId + ":" + consumidor;
    }

    public String getId() {
        return id;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getConsumidor() {
        return consumidor;
    }

    public String getEventType() {
        return eventType;
    }

    public UUID getContratoId() {
        return contratoId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public Instant getProcessadoEm() {
        return processadoEm;
    }
}
