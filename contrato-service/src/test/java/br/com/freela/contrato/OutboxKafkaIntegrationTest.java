package br.com.freela.contrato;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.events.EventoHeaders;
import br.com.freela.common.events.EventoTipos;
import br.com.freela.common.events.KafkaTopicos;
import br.com.freela.common.json.EventoJson;
import br.com.freela.contrato.application.ContratoApplicationService;
import br.com.freela.contrato.application.CriarContratoCommand;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.infrastructure.outbox.MensagemOutboxRepository;
import br.com.freela.contrato.infrastructure.outbox.StatusOutbox;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

/**
 * Verifica o caminho completo do produtor contra um broker real (embutido): transacao de negocio
 * grava na outbox, relay publica, mensagem chega no topico com chave e headers corretos e a ordem
 * dos eventos de um mesmo contrato e preservada.
 */
@SpringBootTest
@EmbeddedKafka(
        partitions = KafkaTopicos.PARTICOES,
        topics = {KafkaTopicos.CONTRATOS_EVENTOS, KafkaTopicos.CONTRATOS_EVENTOS_DLT})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class OutboxKafkaIntegrationTest {

    @Autowired
    private ContratoApplicationService service;

    @Autowired
    private MensagemOutboxRepository outbox;

    @Autowired
    private EventoJson json;

    @Autowired
    private EmbeddedKafkaBroker broker;

    private Consumer<String, String> consumidor;

    @BeforeEach
    void abrirConsumidor() {
        var props = KafkaTestUtils.consumerProps(broker.getBrokersAsString(), "teste-" + UUID.randomUUID(), "true");
        consumidor = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
        broker.consumeFromAnEmbeddedTopic(consumidor, KafkaTopicos.CONTRATOS_EVENTOS);
    }

    @AfterEach
    void fecharConsumidor() {
        if (consumidor != null) {
            consumidor.close();
        }
    }

    @Test
    @DisplayName("criar um contrato grava na outbox e o relay publica no topico")
    void criarPublicaNoTopico() {
        Contrato contrato = criarContrato();

        List<ConsumerRecord<String, String>> registros = aguardarRegistros(contrato.id(), 1);

        ConsumerRecord<String, String> registro = registros.getFirst();
        assertThat(registro.topic()).isEqualTo(KafkaTopicos.CONTRATOS_EVENTOS);
        assertThat(registro.key()).isEqualTo(contrato.id().toString());

        EventoEnvelope envelope = json.lerEnvelope(registro.value());
        assertThat(envelope.eventType()).isEqualTo(EventoTipos.CONTRATO_CRIADO);
        assertThat(envelope.contratoId()).isEqualTo(contrato.id());
        assertThat(envelope.eventId()).isNotNull();
        assertThat(envelope.occurredAt()).isNotNull();

        assertThat(header(registro, EventoHeaders.EVENT_TYPE)).isEqualTo(EventoTipos.CONTRATO_CRIADO);
        assertThat(header(registro, EventoHeaders.CONTRATO_ID)).isEqualTo(contrato.id().toString());
        assertThat(header(registro, EventoHeaders.EVENT_ID)).isEqualTo(envelope.eventId().toString());
        assertThat(header(registro, EventoHeaders.CORRELATION_ID)).isNotBlank();
    }

    @Test
    @DisplayName("os eventos de um mesmo contrato chegam na mesma particao e em ordem")
    void ordemPreservadaPorContrato() {
        Contrato contrato = criarContrato();
        service.registrarEntrega(contrato.id());
        service.concluir(contrato.id());

        List<ConsumerRecord<String, String>> registros = aguardarRegistros(contrato.id(), 3);

        assertThat(registros).extracting(r -> json.lerEnvelope(r.value()).eventType())
                .containsExactly(
                        EventoTipos.CONTRATO_CRIADO,
                        EventoTipos.ENTREGA_REGISTRADA,
                        EventoTipos.CONTRATO_CONCLUIDO);
        assertThat(registros).extracting(ConsumerRecord::partition).containsOnly(registros.getFirst().partition());
        assertThat(registros).extracting(ConsumerRecord::offset).isSorted();
    }

    @Test
    @DisplayName("depois de publicada, a linha da outbox fica marcada como PUBLICADO")
    void outboxEhMarcadaComoPublicada() {
        Contrato contrato = criarContrato();

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(outbox.findByContratoIdOrderBySequenciaAsc(contrato.id()))
                        .isNotEmpty()
                        .allSatisfy(m -> {
                            assertThat(m.getStatus()).isEqualTo(StatusOutbox.PUBLICADO);
                            assertThat(m.getPublicadoEm()).isNotNull();
                        }));
    }

    @Test
    @DisplayName("contratos distintos se espalham pelas particoes, permitindo consumo concorrente")
    void contratosDistintosUsamParticoesDiferentes() {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            ids.add(criarContrato().id());
        }

        List<ConsumerRecord<String, String>> registros = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            registros.addAll(consumir());
            assertThat(registros).hasSizeGreaterThanOrEqualTo(12);
        });

        // Chaves diferentes caem em particoes diferentes: e isso que abre espaco para os
        // consumidores processarem contratos distintos em paralelo.
        assertThat(registros.stream().map(ConsumerRecord::partition).distinct().toList())
                .hasSizeGreaterThan(1);
    }

    private Contrato criarContrato() {
        return service.criar(new CriarContratoCommand(
                UUID.randomUUID(), UUID.randomUUID(), "API de pagamentos", new BigDecimal("3500.00")));
    }

    private List<ConsumerRecord<String, String>> aguardarRegistros(UUID contratoId, int quantidade) {
        List<ConsumerRecord<String, String>> acumulados = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            consumir().stream()
                    .filter(r -> contratoId.toString().equals(r.key()))
                    .forEach(acumulados::add);
            assertThat(acumulados).hasSize(quantidade);
        });
        return acumulados;
    }

    private List<ConsumerRecord<String, String>> consumir() {
        List<ConsumerRecord<String, String>> lidos = new ArrayList<>();
        consumidor.poll(Duration.ofMillis(500)).forEach(lidos::add);
        return lidos;
    }

    private String header(ConsumerRecord<String, String> registro, String nome) {
        var header = registro.headers().lastHeader(nome);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
