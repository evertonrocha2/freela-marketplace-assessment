package br.com.freela.common.events;

import java.util.Set;

/**
 * Tipos de evento publicados pelo contrato-service.
 * O valor textual e o contrato entre produtor e consumidores: nao pode mudar sem nova versao do schema.
 */
public final class EventoTipos {

    public static final String CONTRATO_CRIADO = "ContratoCriado";
    public static final String ENTREGA_REGISTRADA = "EntregaRegistrada";
    public static final String CONTRATO_CONCLUIDO = "ContratoConcluido";
    public static final String CONTRATO_CANCELADO = "ContratoCancelado";

    /** Todo o ciclo de vida do contrato. */
    public static final Set<String> TODOS =
            Set.of(CONTRATO_CRIADO, ENTREGA_REGISTRADA, CONTRATO_CONCLUIDO, CONTRATO_CANCELADO);

    private EventoTipos() {
    }
}
