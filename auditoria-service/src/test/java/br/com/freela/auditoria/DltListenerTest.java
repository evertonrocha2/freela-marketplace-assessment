package br.com.freela.auditoria;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.freela.common.events.KafkaTopicos;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.KafkaHeaders;

@SpringBootTest
class DltListenerTest {

    @Autowired
    private DltListener listener;

    @Autowired
    private EventoFalhaRepository repository;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private DefaultErrorHandler kafkaErrorHandler;

    @Test
    @DisplayName("a mesma copia do DLT entregue duas vezes gera um unico registro de falha")
    void reentregaDoDltNaoDuplicaFalha() {
        long offset = ThreadLocalRandom.current().nextLong(1_000_000, 9_000_000);
        ConsumerRecord<String, String> registro = registroDoDlt(1, offset, "notificacao-service");

        listener.onMensagemComErro(registro);
        listener.onMensagemComErro(registro);

        assertThat(falhasDoOffset(offset)).isEqualTo(1);
    }

    @Test
    @DisplayName("a mesma mensagem invalida falhando nos tres consumidores gera tres falhas")
    void cadaGrupoQueFalhaGeraSuaPropriaFalha() {
        long offset = ThreadLocalRandom.current().nextLong(1_000_000, 9_000_000);

        listener.onMensagemComErro(registroDoDlt(0, offset, "notificacao-service"));
        listener.onMensagemComErro(registroDoDlt(0, offset, "reputacao-service"));
        listener.onMensagemComErro(registroDoDlt(0, offset, "auditoria-service"));

        assertThat(falhasDoOffset(offset)).isEqualTo(3);
        assertThat(repository.findAll().stream()
                .filter(f -> Long.valueOf(offset).equals(f.getOffsetOriginal()))
                .map(EventoFalha::getGrupoConsumidor))
                .containsExactlyInAnyOrder("notificacao-service", "reputacao-service", "auditoria-service");
    }

    @Test
    @DisplayName("falhas de offsets diferentes continuam sendo registradas separadamente")
    void falhasDistintasSaoRegistradas() {
        long offset = ThreadLocalRandom.current().nextLong(1_000_000, 9_000_000);

        listener.onMensagemComErro(registroDoDlt(2, offset, "reputacao-service"));
        listener.onMensagemComErro(registroDoDlt(2, offset + 1, "reputacao-service"));

        assertThat(falhasDoOffset(offset)).isEqualTo(1);
        assertThat(falhasDoOffset(offset + 1)).isEqualTo(1);
    }

    @Test
    @DisplayName("o listener do DLT nao usa o tratador que republica no DLT")
    void listenerDoDltTemTratadorProprio() {
        var principal = (AbstractMessageListenerContainer<?, ?>) registry.getListenerContainer("auditoria-contratos");
        var dlt = (AbstractMessageListenerContainer<?, ?>) registry.getListenerContainer("auditoria-dlt");

        assertThat(principal.getCommonErrorHandler()).isSameAs(kafkaErrorHandler);
        assertThat(dlt.getCommonErrorHandler()).isNotNull().isNotSameAs(kafkaErrorHandler);
    }

    private long falhasDoOffset(long offset) {
        return repository.findAll().stream()
                .filter(f -> Long.valueOf(offset).equals(f.getOffsetOriginal()))
                .count();
    }

    private static ConsumerRecord<String, String> registroDoDlt(int particaoOriginal, long offsetOriginal,
                                                                String grupoConsumidor) {
        var registro = new ConsumerRecord<>(KafkaTopicos.CONTRATOS_EVENTOS_DLT, particaoOriginal, 0L,
                "contrato-invalido", "{isto nao e json");
        registro.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC,
                KafkaTopicos.CONTRATOS_EVENTOS.getBytes(StandardCharsets.UTF_8));
        registro.headers().add(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP,
                grupoConsumidor.getBytes(StandardCharsets.UTF_8));
        registro.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION,
                ByteBuffer.allocate(Integer.BYTES).putInt(particaoOriginal).array());
        registro.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET,
                ByteBuffer.allocate(Long.BYTES).putLong(offsetOriginal).array());
        registro.headers().add(KafkaHeaders.DLT_EXCEPTION_FQCN,
                "org.springframework.kafka.support.serializer.DeserializationException".getBytes(StandardCharsets.UTF_8));
        return registro;
    }
}
