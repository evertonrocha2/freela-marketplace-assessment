package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.shared.DomainEvent;
import java.util.UUID;

/** Evento do ciclo de vida do contrato. */
public interface ContratoEvento extends DomainEvent {

    SnapshotContrato contrato();

    /** Preenchido apenas no cancelamento. */
    default String motivo() {
        return null;
    }

    @Override
    default UUID aggregateId() {
        return contrato().contratoId();
    }
}
