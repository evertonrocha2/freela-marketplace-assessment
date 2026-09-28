package br.com.freela.common.correlation;

import java.util.UUID;
import org.slf4j.MDC;

/** Acesso ao correlationId corrente, seja ele vindo de uma request HTTP ou de uma mensagem Kafka. */
public final class CorrelationContext {

    public static String atualOuNovo() {
        return ouNovo(atual());
    }

    public static String atual() {
        return MDC.get(MdcKeys.CORRELATION_ID);
    }

    /** O proprio valor, ou um UUID novo quando ele vier vazio. */
    public static String ouNovo(String correlationId) {
        return (correlationId == null || correlationId.isBlank()) ? UUID.randomUUID().toString() : correlationId;
    }

    private CorrelationContext() {
    }
}
