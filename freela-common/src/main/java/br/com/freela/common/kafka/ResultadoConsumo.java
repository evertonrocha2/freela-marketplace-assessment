package br.com.freela.common.kafka;

/** Desfecho do processamento de uma mensagem. Vai para o log e deixa a deduplicacao visivel. */
public enum ResultadoConsumo {

    /** Efeito de negocio aplicado e gravado. */
    APLICADO,

    /** Mensagem ja processada antes; nada foi alterado. */
    DUPLICADO_IGNORADO,

    /** Tipo de evento fora do interesse deste consumidor. */
    IGNORADO_POR_TIPO
}
