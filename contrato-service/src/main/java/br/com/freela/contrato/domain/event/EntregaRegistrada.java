package br.com.freela.contrato.domain.event;

import br.com.freela.common.events.EventoTipos;
import java.time.Instant;
import java.util.UUID;

public record EntregaRegistrada(UUID eventId, Instant occurredAt, SnapshotContrato contrato) implements ContratoEvento {

    public static EntregaRegistrada de(SnapshotContrato contrato) {
        return new EntregaRegistrada(UUID.randomUUID(), Instant.now(), contrato);
    }

    @Override
    public String eventType() {
        return EventoTipos.ENTREGA_REGISTRADA;
    }
}
