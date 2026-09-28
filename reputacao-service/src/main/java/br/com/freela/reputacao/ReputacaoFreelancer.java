package br.com.freela.reputacao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "reputacoes")
public class ReputacaoFreelancer {

    @Id
    private UUID freelancerId;

    @Column(nullable = false)
    private int contratosConcluidos;

    @Column(nullable = false)
    private int contratosCancelados;

    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal valorTotal = BigDecimal.ZERO;

    /** Ultimo evento aplicado. Deixa visivel, no proprio agregado, qual mensagem produziu o estado. */
    private UUID ultimoEventoId;

    private Instant atualizadoEm;

    protected ReputacaoFreelancer() {
    }

    public ReputacaoFreelancer(UUID freelancerId) {
        this.freelancerId = freelancerId;
        this.contratosConcluidos = 0;
        this.contratosCancelados = 0;
        this.valorTotal = BigDecimal.ZERO;
    }

    public void registrarConclusao(BigDecimal valor, UUID eventId) {
        this.contratosConcluidos++;
        this.valorTotal = this.valorTotal.add(valor == null ? BigDecimal.ZERO : valor);
        this.ultimoEventoId = eventId;
        this.atualizadoEm = Instant.now();
    }

    public void registrarCancelamento(UUID eventId) {
        this.contratosCancelados++;
        this.ultimoEventoId = eventId;
        this.atualizadoEm = Instant.now();
    }

    public UUID getFreelancerId() {
        return freelancerId;
    }

    public int getContratosConcluidos() {
        return contratosConcluidos;
    }

    public int getContratosCancelados() {
        return contratosCancelados;
    }

    public BigDecimal getValorTotal() {
        return valorTotal;
    }

    public UUID getUltimoEventoId() {
        return ultimoEventoId;
    }

    public Instant getAtualizadoEm() {
        return atualizadoEm;
    }
}
