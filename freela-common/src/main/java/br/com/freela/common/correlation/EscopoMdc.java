package br.com.freela.common.correlation;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
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
            colocar(chave, valor);
        });
    }

    public static EscopoMdc de(Map<String, String> valores) {
        return new EscopoMdc(valores);
    }

    /** Escopo com as quatro chaves que ligam, no Loki, as linhas de log de um mesmo evento. */
    public static EscopoMdc deEvento(String correlationId, Object eventId, String eventType, Object contratoId) {
        return new EscopoMdc(contextoDeEvento(correlationId, eventId, eventType, contratoId));
    }

    /** As quatro chaves de um evento, na ordem em que aparecem no log. Valor nulo fica fora do MDC. */
    public static Map<String, String> contextoDeEvento(String correlationId, Object eventId, String eventType,
                                                       Object contratoId) {
        Map<String, String> valores = new LinkedHashMap<>();
        valores.put(MdcKeys.CORRELATION_ID, correlationId);
        valores.put(MdcKeys.EVENT_ID, Objects.toString(eventId, null));
        valores.put(MdcKeys.EVENT_TYPE, eventType);
        valores.put(MdcKeys.CONTRATO_ID, Objects.toString(contratoId, null));
        return valores;
    }

    @Override
    public void close() {
        anterior.forEach(EscopoMdc::colocar);
    }

    private static void colocar(String chave, String valor) {
        if (valor == null) {
            MDC.remove(chave);
        } else {
            MDC.put(chave, valor);
        }
    }
}
