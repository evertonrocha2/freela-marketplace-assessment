package br.com.freela.common.events;

/**
 * Headers Kafka usados no envelope. O payload continua sendo a fonte da verdade;
 * os headers existem para permitir filtro/roteamento e diagnostico sem desserializar o corpo.
 */
public final class EventoHeaders {

    public static final String EVENT_ID = "eventId";
    public static final String EVENT_TYPE = "eventType";
    public static final String EVENT_VERSION = "eventVersion";
    public static final String CONTRATO_ID = "contratoId";
    public static final String CORRELATION_ID = "X-Correlation-Id";
    public static final String OCCURRED_AT = "occurredAt";
    public static final String PRODUCER = "producer";
    /** Contexto de trace W3C, propagado do HTTP ate o consumidor atraves do Kafka. */
    public static final String TRACEPARENT = "traceparent";

    private EventoHeaders() {
    }
}
