package br.com.freela.common.events;

/**
 * Tipos de evento publicados pelo contrato-service.
 * O valor textual e o contrato entre produtor e consumidores: nao pode mudar sem nova versao do schema.
 */
public final class EventoTipos {

    public static final String CONTRATO_CRIADO = "ContratoCriado";
    public static final String ENTREGA_REGISTRADA = "EntregaRegistrada";
    public static final String CONTRATO_CONCLUIDO = "ContratoConcluido";
    public static final String CONTRATO_CANCELADO = "ContratoCancelado";

    private EventoTipos() {
    }
}
