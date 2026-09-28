package br.com.freela.common.events;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Os campos marcados como obrigatorios em docs/EVENTOS.md precisam ser cobrados pelo codigo. */
class ContratoDeMensagemTest {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private static final UUID CONTRATO = UUID.randomUUID();

    @Test
    @DisplayName("envelope completo e aceito")
    void envelopeCompleto() {
        assertThatCode(() -> envelope("correlacao-1", "contrato-service", payloadJson())).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("envelope sem correlationId e rejeitado")
    void envelopeSemCorrelationId() {
        assertThatThrownBy(() -> envelope(null, "contrato-service", payloadJson()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("correlationId");
    }

    @Test
    @DisplayName("envelope sem producer e rejeitado")
    void envelopeSemProducer() {
        assertThatThrownBy(() -> envelope("correlacao-1", " ", payloadJson()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("producer");
    }

    @Test
    @DisplayName("envelope sem payload e rejeitado")
    void envelopeSemPayload() {
        assertThatThrownBy(() -> envelope("correlacao-1", "contrato-service", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payload");
    }

    @Test
    @DisplayName("payload sem freelancerId e rejeitado")
    void payloadSemFreelancer() {
        assertThatThrownBy(() -> new ContratoEventoPayload(CONTRATO, UUID.randomUUID(), null,
                "API", new BigDecimal("10.00"), "ATIVO", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("freelancerId");
    }

    @Test
    @DisplayName("motivo e o unico campo opcional do payload")
    void motivoEhOpcional() {
        assertThatCode(() -> new ContratoEventoPayload(CONTRATO, UUID.randomUUID(), UUID.randomUUID(),
                "API", new BigDecimal("10.00"), "ATIVO", null)).doesNotThrowAnyException();
    }

    private static EventoEnvelope envelope(String correlationId, String producer, JsonNode payload) {
        return new EventoEnvelope(UUID.randomUUID(), EventoTipos.CONTRATO_CRIADO, EventoEnvelope.VERSAO_ATUAL,
                "Contrato", CONTRATO, CONTRATO, Instant.now(), correlationId, null, producer, payload);
    }

    private static JsonNode payloadJson() {
        return MAPPER.valueToTree(new ContratoEventoPayload(CONTRATO, UUID.randomUUID(), UUID.randomUUID(),
                "API", new BigDecimal("10.00"), "ATIVO", null));
    }
}
