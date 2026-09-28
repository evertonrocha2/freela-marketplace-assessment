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

    /** Valida os campos marcados como obrigatorios na especificacao. So {@code motivo} e opcional. */
    public ContratoEventoPayload {
        exigir(contratoId, "contratoId");
        exigir(clienteId, "clienteId");
        exigir(freelancerId, "freelancerId");
        exigir(valor, "valor");
        if (titulo == null || titulo.isBlank()) {
            throw new IllegalArgumentException("titulo e obrigatorio no payload");
        }
        if (status == null || status.isBlank()) {
            throw new IllegalArgumentException("status e obrigatorio no payload");
        }
    }

    private static void exigir(Object valor, String campo) {
        if (valor == null) {
            throw new IllegalArgumentException(campo + " e obrigatorio no payload");
        }
    }
}
