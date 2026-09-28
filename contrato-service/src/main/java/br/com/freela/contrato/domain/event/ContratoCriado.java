package br.com.freela.contrato.domain.event;

import br.com.freela.common.events.EventoTipos;
import java.time.Instant;
import java.util.UUID;

public record ContratoCriado(UUID eventId, Instant occurredAt, SnapshotContrato contrato) implements ContratoEvento {

    public static ContratoCriado de(SnapshotContrato contrato) {
        return new ContratoCriado(UUID.randomUUID(), Instant.now(), contrato);
    }

    @Override
    public String eventType() {
        return EventoTipos.CONTRATO_CRIADO;
    }
}
