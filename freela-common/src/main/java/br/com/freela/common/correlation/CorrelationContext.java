package br.com.freela.common.correlation;

import java.util.UUID;
import org.slf4j.MDC;

/** Acesso ao correlationId corrente, seja ele vindo de uma request HTTP ou de uma mensagem Kafka. */
public final class CorrelationContext {

    public static String atualOuNovo() {
        String atual = MDC.get(MdcKeys.CORRELATION_ID);
        return (atual == null || atual.isBlank()) ? UUID.randomUUID().toString() : atual;
    }

    public static String atual() {
        return MDC.get(MdcKeys.CORRELATION_ID);
    }

    private CorrelationContext() {
    }
}
