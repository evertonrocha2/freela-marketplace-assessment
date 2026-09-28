package br.com.freela.contrato.infrastructure.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.events.EventoTipos;
import br.com.freela.common.events.KafkaTopicos;
import br.com.freela.common.json.EventoJson;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.tracing.ContextoTrace;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Garante o formato do que entra na outbox: e esse conteudo, e nao a classe Java do evento, que
 * vira o contrato publicado no Kafka.
 */
class RegistroDeEventosOutboxTest {

    private MensagemOutboxRepository repository;
    private ContextoTrace contextoTrace;
    private EventoJson json;
    private RegistroDeEventosOutbox registro;

    @BeforeEach
    void setUp() {
        repository = mock(MensagemOutboxRepository.class);
        contextoTrace = mock(ContextoTrace.class);
        json = new EventoJson();
        when(contextoTrace.traceparentAtual()).thenReturn("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        registro = new RegistroDeEventosOutbox(repository, json, contextoTrace);
    }

    @Test
    @DisplayName("a chave da mensagem e o contratoId, que e o que mantem a ordem por contrato")
    void chaveEhOContratoId() {
        Contrato contrato = Contrato.criar(UUID.randomUUID(), UUID.randomUUID(), "API", new BigDecimal("100.00"));

        registro.registrar(contrato.pullDomainEvents(), "correlacao-1");

        MensagemOutbox mensagem = capturar();
        assertThat(mensagem.getChave()).isEqualTo(contrato.id().toString());
        assertThat(mensagem.getTopico()).isEqualTo(KafkaTopicos.CONTRATOS_EVENTOS);
        assertThat(mensagem.getStatus()).isEqualTo(StatusOutbox.PENDENTE);
    }

    @Test
    @DisplayName("o envelope carrega os campos obrigatorios do contrato de mensageria")
    void envelopeTemOsCamposObrigatorios() {
        UUID cliente = UUID.randomUUID();
        UUID freelancer = UUID.randomUUID();
        Contrato contrato = Contrato.criar(cliente, freelancer, "API de pagamentos", new BigDecimal("3500.00"));
        List<DomainEvent> eventos = contrato.pullDomainEvents();

        registro.registrar(eventos, "correlacao-1");

        EventoEnvelope envelope = json.lerEnvelope(capturar().getPayload());
        assertThat(envelope.eventId()).isEqualTo(eventos.getFirst().eventId());
        assertThat(envelope.eventType()).isEqualTo(EventoTipos.CONTRATO_CRIADO);
        assertThat(envelope.eventVersion()).isEqualTo(EventoEnvelope.VERSAO_ATUAL);
        assertThat(envelope.aggregateType()).isEqualTo("Contrato");
        assertThat(envelope.aggregateId()).isEqualTo(contrato.id());
        assertThat(envelope.contratoId()).isEqualTo(contrato.id());
        assertThat(envelope.occurredAt()).isNotNull();
        assertThat(envelope.correlationId()).isEqualTo("correlacao-1");
        assertThat(envelope.producer()).isEqualTo("contrato-service");
    }

    @Test
    @DisplayName("o valor do contrato sobrevive ao ciclo de serializacao sem perder precisao")
    void valorNaoPerdePrecisao() {
        Contrato contrato = Contrato.criar(UUID.randomUUID(), UUID.randomUUID(), "API", new BigDecimal("1234.56"));

        registro.registrar(contrato.pullDomainEvents(), "correlacao-1");

        EventoEnvelope envelope = json.lerEnvelope(capturar().getPayload());
        var payload = json.lerPayload(envelope.payload(), br.com.freela.common.events.ContratoEventoPayload.class);
        assertThat(payload.valor()).isEqualByComparingTo(new BigDecimal("1234.56"));
        assertThat(payload.status()).isEqualTo("ATIVO");
    }

    @Test
    @DisplayName("o traceparent da requisicao fica guardado para reconectar o trace na publicacao")
    void guardaTraceparent() {
        Contrato contrato = Contrato.criar(UUID.randomUUID(), UUID.randomUUID(), "API", new BigDecimal("10.00"));

        registro.registrar(contrato.pullDomainEvents(), "correlacao-1");

        assertThat(capturar().getTraceparent())
                .isEqualTo("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
    }

    private MensagemOutbox capturar() {
        ArgumentCaptor<MensagemOutbox> captor = ArgumentCaptor.forClass(MensagemOutbox.class);
        verify(repository).save(captor.capture());
        return captor.getValue();
    }
}
