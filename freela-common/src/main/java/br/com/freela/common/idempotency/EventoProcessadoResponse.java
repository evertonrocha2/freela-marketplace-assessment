package br.com.freela.common.idempotency;

import java.time.Instant;
import java.util.UUID;

/** Resposta dos endpoints {@code /eventos-processados}, que expoem as marcas de idempotencia. */
public record EventoProcessadoResponse(UUID eventId, String consumidor, String eventType, UUID contratoId,
                                       String correlationId, Instant processadoEm) {

    public static EventoProcessadoResponse de(EventoProcessado e) {
        return new EventoProcessadoResponse(e.getEventId(), e.getConsumidor(), e.getEventType(),
                e.getContratoId(), e.getCorrelationId(), e.getProcessadoEm());
    }
}
