package br.com.freela.contrato.infrastructure.outbox;

public enum StatusOutbox {

    /** Gravada na mesma transacao do agregado, ainda nao publicada. */
    PENDENTE,

    /** Publicacao confirmada pelo broker. */
    PUBLICADO,

    /** Esgotou as tentativas de publicacao; fica retida para diagnostico e reenvio manual. */
    ERRO
}
