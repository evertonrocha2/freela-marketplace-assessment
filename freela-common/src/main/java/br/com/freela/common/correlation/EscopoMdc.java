package br.com.freela.common.correlation;

import br.com.freela.common.events.EventoEnvelope;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.MDC;

/**
 * Escopo de MDC para o processamento de uma mensagem.
 *
 * <p>Sem isso, o correlationId de uma mensagem vazaria para a proxima processada pela mesma thread
 * do container Kafka, que e reaproveitada. O {@code close} restaura exatamente o que havia antes.</p>
 */
public final class EscopoMdc implements AutoCloseable {

    private final Map<String, String> anterior = new LinkedHashMap<>();

    private EscopoMdc(Map<String, String> valores) {
        valores.forEach((chave, valor) -> {
            anterior.put(chave, MDC.get(chave));
            if (valor == null) {
                MDC.remove(chave);
            } else {
                MDC.put(chave, valor);
            }
        });
    }

    public static EscopoMdc de(EventoEnvelope envelope) {
        Map<String, String> valores = new LinkedHashMap<>();
        valores.put(MdcKeys.CORRELATION_ID, envelope.correlationId());
        valores.put(MdcKeys.EVENT_ID, String.valueOf(envelope.eventId()));
        valores.put(MdcKeys.EVENT_TYPE, envelope.eventType());
        valores.put(MdcKeys.CONTRATO_ID, String.valueOf(envelope.contratoId()));
        return new EscopoMdc(valores);
    }

    public static EscopoMdc de(Map<String, String> valores) {
        return new EscopoMdc(valores);
    }

    @Override
    public void close() {
        anterior.forEach((chave, valor) -> {
            if (valor == null) {
                MDC.remove(chave);
            } else {
                MDC.put(chave, valor);
            }
        });
    }
}
