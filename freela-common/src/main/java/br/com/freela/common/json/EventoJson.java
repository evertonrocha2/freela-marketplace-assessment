package br.com.freela.common.json;

import br.com.freela.common.events.EventoEnvelope;
import org.springframework.stereotype.Component;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Serializacao das mensagens de dominio.
 *
 * <p>O mapper e proprio (nao e um bean {@code ObjectMapper}) de proposito: expor um bean desse tipo
 * faria o Spring Boot desistir do mapper que ele configura para o HTTP, acoplando os dois contratos.
 * Aqui o formato do evento fica congelado independentemente de como a API REST serializa.</p>
 *
 * <p>{@code USE_BIG_DECIMAL_FOR_FLOATS} evita que o valor do contrato perca precisao ao passar
 * por {@code double} na desserializacao. Datas saem em ISO-8601, que ja e o padrao do Jackson 3
 * usado pelo Spring Boot 4.</p>
 */
@Component
public class EventoJson {

    private final ObjectMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    public String escrever(Object valor) {
        return mapper.writeValueAsString(valor);
    }

    public EventoEnvelope lerEnvelope(String json) {
        return mapper.readValue(json, EventoEnvelope.class);
    }

    public <T> T lerPayload(JsonNode payload, Class<T> tipo) {
        return mapper.treeToValue(payload, tipo);
    }

    public JsonNode paraArvore(Object valor) {
        return mapper.valueToTree(valor);
    }
}
