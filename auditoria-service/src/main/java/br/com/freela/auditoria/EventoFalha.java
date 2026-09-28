package br.com.freela.auditoria;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.time.Instant;
import java.util.UUID;

/**
 * Mensagem que esgotou as tentativas em algum consumidor e foi parar no DLT.
 *
 * <p>A mensagem continua no topico DLT do Kafka; esta tabela existe para dar consulta por contrato
 * e correlationId, guardar o motivo da falha junto do corpo original e permitir o reenvio
 * controlado pela API.</p>
 */
@Entity
@Table(name = "auditoria_falhas",
        indexes = {
                @Index(name = "ix_falhas_contrato", columnList = "contratoId"),
                @Index(name = "ix_falhas_correlation", columnList = "correlationId"),
                @Index(name = "ix_falhas_event", columnList = "eventId")
        },
        // Uma falha e identificada pela posicao de onde a mensagem saiu e pelo grupo que falhou.
        // A mesma mensagem invalida e lida pelos tres grupos, e cada um manda a sua copia ao DLT:
        // sao tres falhas distintas, nao duplicatas. Ultima barreira contra registrar duas vezes a
        // mesma falha; a checagem principal fica no DltListener.
        uniqueConstraints = @UniqueConstraint(name = "uk_falhas_origem_grupo",
                columnNames = {"topicoOriginal", "particaoOriginal", "offsetOriginal", "grupoConsumidor"}))
public class EventoFalha {

    @Id
    private UUID id;

    private UUID eventId;

    @Column(length = 60)
    private String eventType;

    private UUID contratoId;

    @Column(length = 120)
    private String correlationId;

    @Column(length = 120)
    private String topicoOriginal;

    private Integer particaoOriginal;

    private Long offsetOriginal;

    /** Grupo consumidor que esgotou as tentativas, ou seja, qual servico nao conseguiu processar. */
    @Column(length = 120)
    private String grupoConsumidor;

    @Column(length = 250)
    private String excecao;

    @Column(columnDefinition = "text")
    private String mensagemErro;

    @Column(columnDefinition = "text")
    private String payload;

    @Column(length = 120)
    private String chave;

    @Column(nullable = false)
    private Instant recebidoEm;

    private Instant reenviadoEm;

    protected EventoFalha() {
    }

    public EventoFalha(UUID eventId, String eventType, UUID contratoId, String correlationId, String topicoOriginal,
                       Integer particaoOriginal, Long offsetOriginal, String grupoConsumidor, String excecao,
                       String mensagemErro, String chave, String payload) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.eventType = eventType;
        this.contratoId = contratoId;
        this.correlationId = correlationId;
        this.topicoOriginal = topicoOriginal;
        this.particaoOriginal = particaoOriginal;
        this.offsetOriginal = offsetOriginal;
        this.grupoConsumidor = grupoConsumidor;
        this.excecao = excecao;
        this.mensagemErro = mensagemErro;
        this.chave = chave;
        this.payload = payload;
        this.recebidoEm = Instant.now();
    }

    public void marcarReenviado() {
        this.reenviadoEm = Instant.now();
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

    public UUID getContratoId() {
        return contratoId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getTopicoOriginal() {
        return topicoOriginal;
    }

    public Integer getParticaoOriginal() {
        return particaoOriginal;
    }

    public Long getOffsetOriginal() {
        return offsetOriginal;
    }

    public String getGrupoConsumidor() {
        return grupoConsumidor;
    }

    public String getExcecao() {
        return excecao;
    }

    public String getMensagemErro() {
        return mensagemErro;
    }

    public String getPayload() {
        return payload;
    }

    public String getChave() {
        return chave;
    }

    public Instant getRecebidoEm() {
        return recebidoEm;
    }

    public Instant getReenviadoEm() {
        return reenviadoEm;
    }
}
