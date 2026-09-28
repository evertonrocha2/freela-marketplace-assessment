package br.com.freela.notificacao;

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
class NotificacaoIdempotenciaTest {

    @Autowired
    private NotificacaoService service;

    @Autowired
    private NotificacaoRepository repository;

    @Autowired
    private EventoJson json;

    @Test
    @DisplayName("reprocessar a mesma mensagem nao cria uma segunda notificacao")
    void mensagemDuplicadaNaoDuplicaNotificacao() {
        EventoEnvelope evento = envelope(EventoTipos.CONTRATO_CRIADO, UUID.randomUUID());

        assertThat(service.processar(evento)).isEqualTo(ResultadoConsumo.APLICADO);
        assertThat(service.processar(evento)).isEqualTo(ResultadoConsumo.DUPLICADO_IGNORADO);
        assertThat(service.processar(evento)).isEqualTo(ResultadoConsumo.DUPLICADO_IGNORADO);

        assertThat(repository.findByContratoIdOrderByCriadaEmAsc(evento.contratoId())).hasSize(1);
    }

    @Test
    @DisplayName("eventos distintos do mesmo contrato geram notificacoes distintas")
    void eventosDistintosGeramNotificacoes() {
        UUID contratoId = UUID.randomUUID();

        service.processar(envelope(EventoTipos.CONTRATO_CRIADO, contratoId));
        service.processar(envelope(EventoTipos.ENTREGA_REGISTRADA, contratoId));
        service.processar(envelope(EventoTipos.CONTRATO_CONCLUIDO, contratoId));

        assertThat(repository.findByContratoIdOrderByCriadaEmAsc(contratoId))
                .extracting(Notificacao::getTipo)
                .containsExactly(
                        EventoTipos.CONTRATO_CRIADO,
                        EventoTipos.ENTREGA_REGISTRADA,
                        EventoTipos.CONTRATO_CONCLUIDO);
    }

    @Test
    @DisplayName("a entrega avisa o cliente; os demais eventos avisam o freelancer")
    void destinatarioDependeDoEvento() {
        UUID contratoId = UUID.randomUUID();
        UUID cliente = UUID.randomUUID();
        UUID freelancer = UUID.randomUUID();

        service.processar(envelope(EventoTipos.CONTRATO_CRIADO, contratoId, cliente, freelancer));
        service.processar(envelope(EventoTipos.ENTREGA_REGISTRADA, contratoId, cliente, freelancer));

        assertThat(repository.findByContratoIdOrderByCriadaEmAsc(contratoId))
                .extracting(Notificacao::getDestinatarioId)
                .containsExactly(freelancer, cliente);
    }

    private EventoEnvelope envelope(String tipo, UUID contratoId) {
        return envelope(tipo, contratoId, UUID.randomUUID(), UUID.randomUUID());
    }

    private EventoEnvelope envelope(String tipo, UUID contratoId, UUID cliente, UUID freelancer) {
        var payload = new ContratoEventoPayload(contratoId, cliente, freelancer,
                "API de pagamentos", new BigDecimal("3500.00"), "ATIVO", null);
        return new EventoEnvelope(UUID.randomUUID(), tipo, EventoEnvelope.VERSAO_ATUAL, "Contrato",
                contratoId, contratoId, Instant.now(), "correlacao-teste", null, "contrato-service",
                json.paraArvore(payload));
    }
}
