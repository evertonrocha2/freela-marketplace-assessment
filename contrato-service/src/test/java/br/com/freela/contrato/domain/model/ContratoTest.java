package br.com.freela.contrato.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import br.com.freela.common.events.EventoTipos;
import br.com.freela.contrato.domain.event.ContratoCancelado;
import br.com.freela.contrato.domain.shared.DomainEvent;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ContratoTest {

    private static final UUID CLIENTE = UUID.randomUUID();
    private static final UUID FREELANCER = UUID.randomUUID();
    private static final BigDecimal VALOR = new BigDecimal("3500.00");

    private Contrato contratoAtivo() {
        return Contrato.criar(CLIENTE, FREELANCER, "API de pagamentos", VALOR);
    }

    @Test
    @DisplayName("criar produz ContratoCriado com o estado do contrato")
    void criarProduzEvento() {
        Contrato contrato = contratoAtivo();

        List<DomainEvent> eventos = contrato.pullDomainEvents();

        assertThat(contrato.status()).isEqualTo(StatusContrato.ATIVO);
        assertThat(eventos).hasSize(1);
        assertThat(eventos.getFirst().eventType()).isEqualTo(EventoTipos.CONTRATO_CRIADO);
        assertThat(eventos.getFirst().aggregateId()).isEqualTo(contrato.id());
        assertThat(eventos.getFirst().eventId()).isNotNull();
        assertThat(eventos.getFirst().occurredAt()).isNotNull();
    }

    @Test
    @DisplayName("o fluxo completo gera os eventos na ordem das transicoes")
    void fluxoCompletoGeraEventosEmOrdem() {
        Contrato contrato = contratoAtivo();
        contrato.registrarEntrega();
        contrato.concluir();

        List<String> tipos = contrato.pullDomainEvents().stream().map(DomainEvent::eventType).toList();

        assertThat(tipos).containsExactly(
                EventoTipos.CONTRATO_CRIADO,
                EventoTipos.ENTREGA_REGISTRADA,
                EventoTipos.CONTRATO_CONCLUIDO);
        assertThat(contrato.status()).isEqualTo(StatusContrato.CONCLUIDO);
    }

    @Test
    @DisplayName("pullDomainEvents esvazia a lista para nao republicar o mesmo evento")
    void pullEsvaziaOsEventos() {
        Contrato contrato = contratoAtivo();

        assertThat(contrato.pullDomainEvents()).hasSize(1);
        assertThat(contrato.pullDomainEvents()).isEmpty();
    }

    @Test
    @DisplayName("concluir sem entrega registrada e recusado")
    void concluirSemEntrega() {
        Contrato contrato = contratoAtivo();

        assertThatThrownBy(contrato::concluir)
                .isInstanceOf(TransicaoInvalidaException.class);
        assertThat(contrato.status()).isEqualTo(StatusContrato.ATIVO);
    }

    @Test
    @DisplayName("entrega so e aceita em contrato ativo")
    void entregaSomenteEmContratoAtivo() {
        Contrato contrato = contratoAtivo();
        contrato.registrarEntrega();

        assertThatThrownBy(contrato::registrarEntrega)
                .isInstanceOf(TransicaoInvalidaException.class);
    }

    @Test
    @DisplayName("contrato concluido nao pode ser cancelado")
    void concluidoNaoCancela() {
        Contrato contrato = contratoAtivo();
        contrato.registrarEntrega();
        contrato.concluir();

        assertThatThrownBy(() -> contrato.cancelar("desistencia"))
                .isInstanceOf(TransicaoInvalidaException.class);
    }

    @Test
    @DisplayName("o cancelamento leva o motivo no evento")
    void cancelamentoCarregaMotivo() {
        Contrato contrato = contratoAtivo();
        contrato.pullDomainEvents();

        contrato.cancelar("cliente desistiu");

        List<DomainEvent> eventos = contrato.pullDomainEvents();
        assertThat(eventos).hasSize(1);
        assertThat(eventos.getFirst()).isInstanceOf(ContratoCancelado.class);
        assertThat(((ContratoCancelado) eventos.getFirst()).motivo()).isEqualTo("cliente desistiu");
        assertThat(contrato.status()).isEqualTo(StatusContrato.CANCELADO);
    }

    @Test
    @DisplayName("valor precisa ser positivo")
    void valorPrecisaSerPositivo() {
        assertThatThrownBy(() -> Contrato.criar(CLIENTE, FREELANCER, "Trabalho", BigDecimal.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
