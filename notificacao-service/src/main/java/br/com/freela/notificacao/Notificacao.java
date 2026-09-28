package br.com.freela.notificacao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notificacoes",
        indexes = {
                @Index(name = "ix_notificacoes_contrato", columnList = "contratoId"),
                @Index(name = "ix_notificacoes_event", columnList = "eventId")
        })
public class Notificacao {

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID contratoId;

    @Column(nullable = false)
    private UUID destinatarioId;

    @Column(nullable = false, length = 60)
    private String tipo;

    @Column(length = 500)
    private String mensagem;

    /** Evento que originou a notificacao. Torna a deduplicacao auditavel a olho nu. */
    @Column(nullable = false)
    private UUID eventId;

    @Column(length = 120)
    private String correlationId;

    @Column(nullable = false)
    private Instant criadaEm;

    protected Notificacao() {
    }

    public Notificacao(UUID contratoId, UUID destinatarioId, String tipo, String mensagem,
                       UUID eventId, String correlationId) {
        this.id = UUID.randomUUID();
        this.contratoId = contratoId;
        this.destinatarioId = destinatarioId;
        this.tipo = tipo;
        this.mensagem = mensagem;
        this.eventId = eventId;
        this.correlationId = correlationId;
        this.criadaEm = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public UUID getContratoId() {
        return contratoId;
    }

    public UUID getDestinatarioId() {
        return destinatarioId;
    }

    public String getTipo() {
        return tipo;
    }

    public String getMensagem() {
        return mensagem;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public Instant getCriadaEm() {
        return criadaEm;
    }
}
