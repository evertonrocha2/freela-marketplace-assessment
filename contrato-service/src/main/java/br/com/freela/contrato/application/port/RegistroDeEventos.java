package br.com.freela.contrato.application.port;

import br.com.freela.contrato.domain.shared.DomainEvent;
import java.util.List;

/**
 * Porta de saida usada pela camada de aplicacao para entregar eventos de dominio.
 *
 * <p>A implementacao grava na outbox, sempre dentro da transacao que esta em curso. A aplicacao
 * nao conhece Kafka.</p>
 */
public interface RegistroDeEventos {

    void registrar(List<DomainEvent> eventos, String correlationId);
}
