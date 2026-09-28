package br.com.freela.common.events;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Corpo de negocio dos eventos de ciclo de vida do contrato.
 *
 * <p>Todos os quatro eventos usam este mesmo formato de payload. Ele carrega os dados que os
 * consumidores precisam para agir sem precisar chamar o contrato-service por HTTP
 * (notificacao precisa de cliente/freelancer/titulo, reputacao precisa de freelancer/valor).</p>
 *
 * <p>{@code motivo} so e preenchido em {@code ContratoCancelado}; nos demais eventos vem nulo.</p>
 */
public record ContratoEventoPayload(
        UUID contratoId,
        UUID clienteId,
        UUID freelancerId,
        String titulo,
        BigDecimal valor,
        String status,
        String motivo) {

    public ContratoEventoPayload {
        if (contratoId == null) {
            throw new IllegalArgumentException("contratoId e obrigatorio no payload");
        }
    }
}
