package br.com.freela.common.kafka;

import br.com.freela.common.correlation.MdcKeys;
import br.com.freela.common.events.EventoHeaders;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

public final class HeadersKafka {

    public static String ler(Headers headers, String nome) {
        var header = headers.lastHeader(nome);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * Contexto de log montado a partir dos headers, antes de desserializar o corpo.
     *
     * <p>A ordem importa: se o payload estiver corrompido, a linha de erro ainda sai com
     * correlationId e eventId, que e justamente o caso em que rastrear e mais necessario.</p>
     */
    public static Map<String, String> mdc(ConsumerRecord<String, String> registro) {
        Map<String, String> valores = new LinkedHashMap<>();
        valores.put(MdcKeys.CORRELATION_ID, ler(registro.headers(), EventoHeaders.CORRELATION_ID));
        valores.put(MdcKeys.EVENT_ID, ler(registro.headers(), EventoHeaders.EVENT_ID));
        valores.put(MdcKeys.EVENT_TYPE, ler(registro.headers(), EventoHeaders.EVENT_TYPE));
        valores.put(MdcKeys.CONTRATO_ID, ler(registro.headers(), EventoHeaders.CONTRATO_ID));
        return valores;
    }

    private HeadersKafka() {
    }
}
