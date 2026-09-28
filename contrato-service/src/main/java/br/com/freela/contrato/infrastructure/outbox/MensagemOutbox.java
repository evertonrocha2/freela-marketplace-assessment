package br.com.freela.contrato.infrastructure.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Linha da outbox transacional.
 *
 * <p>A chave primaria e {@code sequencia}, uma identity do banco. E ela que define a ordem de
 * publicacao: o relay le sempre ordenado por sequencia. O {@code eventId} e unico e e o mesmo
 * valor que os consumidores usam para deduplicar.</p>
 *
 * <p>{@code payload} guarda o envelope JSON ja montado. Serializar na escrita, e nao na
 * publicacao, faz com que o conteudo da mensagem seja decidido dentro da transacao de negocio:
 * o que foi commitado e exatamente o que sera publicado.</p>
 */
@Entity
@Table(name = "outbox_eventos",
        indexes = {
                @Index(name = "ix_outbox_status_sequencia", columnList = "status,sequencia"),
                @Index(name = "ix_outbox_contrato", columnList = "contratoId"),
                @Index(name = "ix_outbox_correlation", columnList = "correlationId")
        })
public class MensagemOutbox {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long sequencia;

    @Column(nullable = false, unique = true, updatable = false)
    private UUID eventId;

    @Column(nullable = false, length = 60)
    private String eventType;

    @Column(nullable = false)
    private int eventVersion;

    @Column(nullable = false, length = 40)
    private String aggregateType;

    @Column(nullable = false)
    private UUID aggregateId;

    @Column(nullable = false)
    private UUID contratoId;

    @Column(nullable = false, length = 120)
    private String topico;

    @Column(nullable = false, length = 120)
    private String chave;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(length = 120)
    private String correlationId;

    @Column(length = 120)
    private String traceparent;

    @Column(nullable = false)
    private Instant occurredAt;

    @Column(nullable = false)
    private Instant criadoEm;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private StatusOutbox status;

    private Instant publicadoEm;

    @Column(nullable = false)
    private int tentativas;

    @Column(length = 1000)
    private String ultimoErro;

    protected MensagemOutbox() {
    }

    public MensagemOutbox(UUID eventId, String eventType, int eventVersion, String aggregateType,
                          UUID aggregateId, UUID contratoId, String topico, String chave, String payload,
                          String correlationId, String traceparent, Instant occurredAt) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.eventVersion = eventVersion;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.contratoId = contratoId;
        this.topico = topico;
        this.chave = chave;
        this.payload = payload;
        this.correlationId = correlationId;
        this.traceparent = traceparent;
        this.occurredAt = occurredAt;
        this.criadoEm = Instant.now();
        this.status = StatusOutbox.PENDENTE;
        this.tentativas = 0;
    }

    public void marcarPublicado() {
        this.status = StatusOutbox.PUBLICADO;
        this.publicadoEm = Instant.now();
        this.ultimoErro = null;
    }

    public void registrarFalha(String erro, int maxTentativas) {
        this.tentativas++;
        this.ultimoErro = erro == null ? null : erro.substring(0, Math.min(erro.length(), 1000));
        if (this.tentativas >= maxTentativas) {
            this.status = StatusOutbox.ERRO;
        }
    }

    public void reenfileirar() {
        this.status = StatusOutbox.PENDENTE;
        this.tentativas = 0;
        this.ultimoErro = null;
    }

    public Long getSequencia() {
        return sequencia;
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

    public String getTopico() {
        return topico;
    }

    public String getChave() {
        return chave;
    }

    public String getPayload() {
        return payload;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getTraceparent() {
        return traceparent;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public Instant getCriadoEm() {
        return criadoEm;
    }

    public StatusOutbox getStatus() {
        return status;
    }

    public Instant getPublicadoEm() {
        return publicadoEm;
    }

    public int getTentativas() {
        return tentativas;
    }

    public String getUltimoErro() {
        return ultimoErro;
    }
}
