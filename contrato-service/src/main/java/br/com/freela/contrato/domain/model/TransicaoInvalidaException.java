package br.com.freela.contrato.domain.model;

import java.util.UUID;

/** Transicao de estado recusada pelas regras do agregado. Vira HTTP 409 na borda. */
public class TransicaoInvalidaException extends RuntimeException {

    private final UUID contratoId;
    private final StatusContrato statusAtual;
    private final StatusContrato statusPretendido;

    public TransicaoInvalidaException(UUID contratoId, StatusContrato statusAtual, StatusContrato statusPretendido) {
        super("Contrato %s está em %s e não pode ir para %s".formatted(contratoId, statusAtual, statusPretendido));
        this.contratoId = contratoId;
        this.statusAtual = statusAtual;
        this.statusPretendido = statusPretendido;
    }

    public UUID contratoId() {
        return contratoId;
    }

    public StatusContrato statusAtual() {
        return statusAtual;
    }

    public StatusContrato statusPretendido() {
        return statusPretendido;
    }
}
