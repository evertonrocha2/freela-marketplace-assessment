package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.StatusContrato;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * Estado do contrato no instante em que o evento ocorreu.
 *
 * <p>Os eventos carregam o estado inteiro, e nao so o id, para que os consumidores nao precisem
 * chamar o contrato-service de volta por HTTP. Sem isso, a comunicacao seria assincrona so na
 * aparencia: cada consumidor criaria um acoplamento sincrono na hora de processar.</p>
 */
public record SnapshotContrato(
        UUID contratoId,
        UUID clienteId,
        UUID freelancerId,
        String titulo,
        BigDecimal valor,
        StatusContrato status) {
}
