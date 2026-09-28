package br.com.freela.common.kafka;

import br.com.freela.common.correlation.EscopoMdc;
import br.com.freela.common.events.EventoHeaders;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/** Leitura e escrita dos headers Kafka, no mesmo formato para produtor, consumidores e DLT. */
public final class HeadersKafka {

    public static String ler(Headers headers, String nome) {
        Header header = headers.lastHeader(nome);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** Headers binarios de inteiro, como a particao de origem que o Spring Kafka grava no DLT. */
    public static Integer lerInt(Headers headers, String nome) {
        ByteBuffer valor = binario(headers, nome, Integer.BYTES);
        return valor == null ? null : valor.getInt();
    }

    /** Headers binarios de long, como o offset de origem que o Spring Kafka grava no DLT. */
    public static Long lerLong(Headers headers, String nome) {
        ByteBuffer valor = binario(headers, nome, Long.BYTES);
        return valor == null ? null : valor.getLong();
    }

    /** Grava o valor como texto UTF-8. Valor nulo nao vira header. */
    public static void escrever(Headers headers, String nome, Object valor) {
        if (valor != null) {
            headers.add(nome, valor.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Contexto de log montado a partir dos headers, antes de desserializar o corpo.
     *
     * <p>A ordem importa: se o payload estiver corrompido, a linha de erro ainda sai com
     * correlationId e eventId, que e justamente o caso em que rastrear e mais necessario.</p>
     */
    public static EscopoMdc escopoDeLog(ConsumerRecord<String, String> registro) {
        Headers headers = registro.headers();
        return EscopoMdc.deEvento(
                ler(headers, EventoHeaders.CORRELATION_ID),
                ler(headers, EventoHeaders.EVENT_ID),
                ler(headers, EventoHeaders.EVENT_TYPE),
                ler(headers, EventoHeaders.CONTRATO_ID));
    }

    private static ByteBuffer binario(Headers headers, String nome, int tamanho) {
        Header header = headers.lastHeader(nome);
        if (header == null || header.value() == null || header.value().length < tamanho) {
            return null;
        }
        return ByteBuffer.wrap(header.value());
    }

    private HeadersKafka() {
    }
}
