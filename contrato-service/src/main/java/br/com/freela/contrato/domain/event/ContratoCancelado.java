package br.com.freela.contrato.domain.event;

import br.com.freela.common.events.EventoTipos;
import java.time.Instant;
import java.util.UUID;

public record ContratoCancelado(UUID eventId, Instant occurredAt, SnapshotContrato contrato, String motivo)
        implements ContratoEvento {

    public static ContratoCancelado de(SnapshotContrato contrato, String motivo) {
        return new ContratoCancelado(UUID.randomUUID(), Instant.now(), contrato, motivo);
    }

    @Override
    public String eventType() {
        return EventoTipos.CONTRATO_CANCELADO;
    }
}
