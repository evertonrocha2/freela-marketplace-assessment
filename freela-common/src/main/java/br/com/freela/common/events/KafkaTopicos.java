package br.com.freela.common.events;

/**
 * Topicos Kafka da plataforma.
 *
 * <p>Optou-se por um unico topico para todo o ciclo de vida do contrato. Isso e o que permite
 * garantir ordem total dos eventos de um mesmo contrato: a chave da mensagem e o {@code contratoId},
 * logo todos os eventos do contrato caem na mesma particao e sao entregues em ordem.
 * Topicos separados por tipo de evento nao dariam essa garantia.</p>
 */
public final class KafkaTopicos {

    /** Topico principal do ciclo de vida do contrato. Chave = contratoId. */
    public static final String CONTRATOS_EVENTOS = "freela.contratos.eventos";

    /** Dead letter topic: mensagens que esgotaram as tentativas de processamento. */
    public static final String CONTRATOS_EVENTOS_DLT = "freela.contratos.eventos.dlt";

    /** Numero de particoes do topico principal (e do DLT, para manter o mapeamento de particao). */
    public static final int PARTICOES = 3;

    private KafkaTopicos() {
    }
}
