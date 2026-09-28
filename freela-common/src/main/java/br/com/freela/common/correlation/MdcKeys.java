package br.com.freela.common.correlation;

/**
 * Chaves de MDC. Aparecem em todas as linhas de log em JSON e viram campos pesquisaveis no Loki,
 * o que e o que permite seguir uma operacao entre servicos sem abrir o console de cada um.
 */
public final class MdcKeys {

    public static final String CORRELATION_ID = "correlationId";
    public static final String CONTRATO_ID = "contratoId";
    public static final String EVENT_ID = "eventId";
    public static final String EVENT_TYPE = "eventType";

    private MdcKeys() {
    }
}
