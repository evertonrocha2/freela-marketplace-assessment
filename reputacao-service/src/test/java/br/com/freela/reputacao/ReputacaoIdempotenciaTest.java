package br.com.freela.reputacao;

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
class ReputacaoIdempotenciaTest {

    @Autowired
    private ReputacaoService service;

    @Autowired
    private ReputacaoRepository repository;

    @Autowired
    private EventoJson json;

    @Test
    @DisplayName("reprocessar ContratoConcluido nao incrementa o contador duas vezes")
    void duplicadoNaoIncrementaDuasVezes() {
        UUID freelancer = UUID.randomUUID();
        EventoEnvelope evento = envelope(EventoTipos.CONTRATO_CONCLUIDO, freelancer, new BigDecimal("1000.00"));

        assertThat(service.processar(evento)).isEqualTo(ResultadoConsumo.APLICADO);
        assertThat(service.processar(evento)).isEqualTo(ResultadoConsumo.DUPLICADO_IGNORADO);
        assertThat(service.processar(evento)).isEqualTo(ResultadoConsumo.DUPLICADO_IGNORADO);

        ReputacaoFreelancer reputacao = repository.findById(freelancer).orElseThrow();
        assertThat(reputacao.getContratosConcluidos()).isEqualTo(1);
        assertThat(reputacao.getValorTotal()).isEqualByComparingTo(new BigDecimal("1000.00"));
    }

    @Test
    @DisplayName("contratos concluidos diferentes somam normalmente")
    void eventosDistintosSomam() {
        UUID freelancer = UUID.randomUUID();

        service.processar(envelope(EventoTipos.CONTRATO_CONCLUIDO, freelancer, new BigDecimal("1000.00")));
        service.processar(envelope(EventoTipos.CONTRATO_CONCLUIDO, freelancer, new BigDecimal("500.50")));

        ReputacaoFreelancer reputacao = repository.findById(freelancer).orElseThrow();
        assertThat(reputacao.getContratosConcluidos()).isEqualTo(2);
        assertThat(reputacao.getValorTotal()).isEqualByComparingTo(new BigDecimal("1500.50"));
    }

    @Test
    @DisplayName("eventos sem impacto na reputacao sao ignorados sem gravar nada")
    void eventoSemImpactoEhIgnorado() {
        UUID freelancer = UUID.randomUUID();

        var resultado = service.processar(envelope(EventoTipos.CONTRATO_CRIADO, freelancer, new BigDecimal("10.00")));

        assertThat(resultado).isEqualTo(ResultadoConsumo.IGNORADO_POR_TIPO);
        assertThat(repository.findById(freelancer)).isEmpty();
    }

    @Test
    @DisplayName("cancelamento conta separado da conclusao")
    void cancelamentoContaSeparado() {
        UUID freelancer = UUID.randomUUID();

        service.processar(envelope(EventoTipos.CONTRATO_CONCLUIDO, freelancer, new BigDecimal("200.00")));
        service.processar(envelope(EventoTipos.CONTRATO_CANCELADO, freelancer, new BigDecimal("300.00")));

        ReputacaoFreelancer reputacao = repository.findById(freelancer).orElseThrow();
        assertThat(reputacao.getContratosConcluidos()).isEqualTo(1);
        assertThat(reputacao.getContratosCancelados()).isEqualTo(1);
        assertThat(reputacao.getValorTotal()).isEqualByComparingTo(new BigDecimal("200.00"));
    }

    private EventoEnvelope envelope(String tipo, UUID freelancer, BigDecimal valor) {
        UUID contratoId = UUID.randomUUID();
        var payload = new ContratoEventoPayload(contratoId, UUID.randomUUID(), freelancer,
                "API de pagamentos", valor, "CONCLUIDO", null);
        return new EventoEnvelope(UUID.randomUUID(), tipo, EventoEnvelope.VERSAO_ATUAL, "Contrato",
                contratoId, contratoId, Instant.now(), "correlacao-teste", null, "contrato-service",
                json.paraArvore(payload));
    }
}
