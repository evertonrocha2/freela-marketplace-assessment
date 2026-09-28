package br.com.freela.auditoria;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.freela.common.events.ContratoEventoPayload;
import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.events.EventoTipos;
import br.com.freela.common.json.EventoJson;
import br.com.freela.common.kafka.ResultadoConsumo;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class AuditoriaIdempotenciaTest {

    @Autowired
    private AuditoriaService service;

    @Autowired
    private EventoAuditoriaRepository repository;

    @Autowired
    private EventoJson json;

    @Test
    @DisplayName("reprocessar a mesma mensagem nao cria um segundo registro de auditoria")
    void duplicadoNaoDuplicaRegistro() {
        EventoEnvelope evento = envelope(EventoTipos.CONTRATO_CRIADO, UUID.randomUUID());

        assertThat(service.processar(evento)).isEqualTo(ResultadoConsumo.APLICADO);
        assertThat(service.processar(evento)).isEqualTo(ResultadoConsumo.DUPLICADO_IGNORADO);

        assertThat(repository.findByEventId(evento.eventId())).hasSize(1);
    }

    @Test
    @DisplayName("a auditoria registra todos os tipos de evento, sem filtrar")
    void registraTodosOsTipos() {
        UUID contratoId = UUID.randomUUID();

        service.processar(envelope(EventoTipos.CONTRATO_CRIADO, contratoId));
        service.processar(envelope(EventoTipos.ENTREGA_REGISTRADA, contratoId));
        service.processar(envelope(EventoTipos.CONTRATO_CONCLUIDO, contratoId));
        service.processar(envelope(EventoTipos.CONTRATO_CANCELADO, contratoId));

        assertThat(repository.findByContratoIdOrderByOccurredAtAsc(contratoId)).hasSize(4);
    }

    @Test
    @DisplayName("o registro guarda os metadados exigidos para consulta posterior")
    void guardaOsMetadadosDeConsulta() {
        UUID contratoId = UUID.randomUUID();
        EventoEnvelope evento = envelope(EventoTipos.CONTRATO_CONCLUIDO, contratoId);

        service.processar(evento);

        EventoAuditoria registro = repository.findByEventId(evento.eventId()).getFirst();
        assertThat(registro.getEventId()).isEqualTo(evento.eventId());
        assertThat(registro.getEventType()).isEqualTo(EventoTipos.CONTRATO_CONCLUIDO);
        assertThat(registro.getContratoId()).isEqualTo(contratoId);
        assertThat(registro.getOccurredAt()).isNotNull();
        assertThat(registro.getCorrelationId()).isEqualTo("correlacao-teste");
        assertThat(registro.getPayload()).contains(contratoId.toString());
    }

    private EventoEnvelope envelope(String tipo, UUID contratoId) {
        var payload = new ContratoEventoPayload(contratoId, UUID.randomUUID(), UUID.randomUUID(),
                "API de pagamentos", new BigDecimal("3500.00"), "ATIVO", null);
        return new EventoEnvelope(UUID.randomUUID(), tipo, EventoEnvelope.VERSAO_ATUAL, "Contrato",
                contratoId, contratoId, Instant.now(), "correlacao-teste", null, "contrato-service",
                json.paraArvore(payload));
    }
}
