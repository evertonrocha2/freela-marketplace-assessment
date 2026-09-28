package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.event.ContratoCancelado;
import br.com.freela.contrato.domain.event.ContratoConcluido;
import br.com.freela.contrato.domain.event.ContratoCriado;
import br.com.freela.contrato.domain.event.EntregaRegistrada;
import br.com.freela.contrato.domain.event.SnapshotContrato;
import br.com.freela.contrato.domain.shared.DomainEvent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Agregado Contrato.
 *
 * <p>Toda transicao de estado valida produz um evento de dominio. Os eventos ficam acumulados no
 * agregado e sao recolhidos pela camada de aplicacao com {@link #pullDomainEvents()}, que grava na
 * outbox dentro da mesma transacao da mudanca de estado.</p>
 */
public class Contrato {

    private final UUID id;
    private final UUID clienteId;
    private final UUID freelancerId;
    private final String titulo;
    private final BigDecimal valor;
    private StatusContrato status;
    private final Instant criadoEm;
    private final List<DomainEvent> domainEvents = new ArrayList<>();

    private Contrato(UUID id, UUID clienteId, UUID freelancerId, String titulo,
                     BigDecimal valor, StatusContrato status, Instant criadoEm) {
        this.id = id;
        this.clienteId = clienteId;
        this.freelancerId = freelancerId;
        this.titulo = titulo;
        this.valor = valor;
        this.status = status;
        this.criadoEm = criadoEm;
    }

    public static Contrato criar(UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor) {
        if (clienteId == null || freelancerId == null) {
            throw new IllegalArgumentException("Cliente e freelancer são obrigatórios");
        }
        if (titulo == null || titulo.isBlank()) {
            throw new IllegalArgumentException("Título é obrigatório");
        }
        if (valor == null || valor.signum() <= 0) {
            throw new IllegalArgumentException("Valor deve ser positivo");
        }
        var contrato = new Contrato(UUID.randomUUID(), clienteId, freelancerId, titulo.trim(), valor,
                StatusContrato.ATIVO, Instant.now());
        contrato.domainEvents.add(ContratoCriado.de(contrato.snapshot()));
        return contrato;
    }

    public static Contrato restaurar(UUID id, UUID clienteId, UUID freelancerId, String titulo,
                                     BigDecimal valor, StatusContrato status, Instant criadoEm) {
        return new Contrato(id, clienteId, freelancerId, titulo, valor, status, criadoEm);
    }

    public void registrarEntrega() {
        if (status != StatusContrato.ATIVO) {
            throw new TransicaoInvalidaException(id, status, StatusContrato.ENTREGA_REGISTRADA);
        }
        status = StatusContrato.ENTREGA_REGISTRADA;
        domainEvents.add(EntregaRegistrada.de(snapshot()));
    }

    public void concluir() {
        if (status != StatusContrato.ENTREGA_REGISTRADA) {
            throw new TransicaoInvalidaException(id, status, StatusContrato.CONCLUIDO);
        }
        status = StatusContrato.CONCLUIDO;
        domainEvents.add(ContratoConcluido.de(snapshot()));
    }

    public void cancelar(String motivo) {
        if (status == StatusContrato.CONCLUIDO || status == StatusContrato.CANCELADO) {
            throw new TransicaoInvalidaException(id, status, StatusContrato.CANCELADO);
        }
        status = StatusContrato.CANCELADO;
        domainEvents.add(ContratoCancelado.de(snapshot(), motivo));
    }

    public SnapshotContrato snapshot() {
        return new SnapshotContrato(id, clienteId, freelancerId, titulo, valor, status);
    }

    public List<DomainEvent> pullDomainEvents() {
        var copia = List.copyOf(domainEvents);
        domainEvents.clear();
        return copia;
    }

    public List<DomainEvent> domainEvents() {
        return Collections.unmodifiableList(domainEvents);
    }

    public UUID id() {
        return id;
    }

    public UUID clienteId() {
        return clienteId;
    }

    public UUID freelancerId() {
        return freelancerId;
    }

    public String titulo() {
        return titulo;
    }

    public BigDecimal valor() {
        return valor;
    }

    public StatusContrato status() {
        return status;
    }

    public Instant criadoEm() {
        return criadoEm;
    }
}
