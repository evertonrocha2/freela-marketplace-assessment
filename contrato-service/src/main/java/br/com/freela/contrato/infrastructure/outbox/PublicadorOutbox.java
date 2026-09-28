package br.com.freela.contrato.infrastructure.outbox;

import br.com.freela.common.correlation.EscopoMdc;
import br.com.freela.common.events.EventoHeaders;
import br.com.freela.common.kafka.HeadersKafka;
import br.com.freela.contrato.infrastructure.tracing.ContextoTrace;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Relay da outbox: le as mensagens pendentes e publica no Kafka.
 *
 * <p>Este componente e o unico ponto do servico que fala com o broker. A transacao de negocio
 * nunca publica diretamente, porque um commit no banco e um send no Kafka nao compartilham
 * transacao: se o send acontecesse dentro do metodo de negocio, uma falha depois do send deixaria
 * o evento publicado sem o dado correspondente no banco, e uma falha antes do commit deixaria o
 * dado gravado sem evento. Com a outbox, o unico commit que importa e o do banco, e a publicacao
 * vira uma consequencia garantida por reentrega (entrega ao menos uma vez).</p>
 *
 * <p><b>Ordem.</b> As pendentes sao lidas por sequencia crescente e publicadas uma a uma, esperando
 * o ack de cada envio. Se um envio falha, o lote e interrompido ali: as mensagens seguintes
 * continuam PENDENTE e serao publicadas na proxima passada, nunca na frente da que falhou. Isso
 * preserva a ordem tambem entre eventos de um mesmo contrato. Quando uma mensagem esgota as
 * tentativas ela vai para ERRO e deixa de ser lida, entao uma mensagem definitivamente ruim nao
 * trava a fila.</p>
 *
 * <p><b>Duplicidade.</b> Se o broker confirmar o envio e o servico cair antes do commit do
 * {@code marcarPublicado}, a mesma mensagem sera publicada de novo. Isso e esperado em entrega ao
 * menos uma vez, e e exatamente o caso que a idempotencia por {@code eventId} nos consumidores
 * cobre.</p>
 */
@Component
public class PublicadorOutbox {

    private static final Logger log = LoggerFactory.getLogger(PublicadorOutbox.class);

    private final MensagemOutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ContextoTrace contextoTrace;
    private final int tamanhoLote;
    private final int maxTentativas;
    private final long timeoutEnvioMs;

    public PublicadorOutbox(MensagemOutboxRepository repository,
                            KafkaTemplate<String, String> kafkaTemplate,
                            ContextoTrace contextoTrace,
                            @Value("${freela.outbox.tamanho-lote:50}") int tamanhoLote,
                            @Value("${freela.outbox.max-tentativas:5}") int maxTentativas,
                            @Value("${freela.outbox.timeout-envio-ms:10000}") long timeoutEnvioMs) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.contextoTrace = contextoTrace;
        this.tamanhoLote = tamanhoLote;
        this.maxTentativas = maxTentativas;
        this.timeoutEnvioMs = timeoutEnvioMs;
    }

    @Scheduled(fixedDelayString = "${freela.outbox.intervalo-ms:300}")
    @Transactional
    public void publicarPendentes() {
        List<MensagemOutbox> pendentes = repository.buscarPendentes(Limit.of(tamanhoLote));
        if (pendentes.isEmpty()) {
            return;
        }
        log.info("outbox.relay.inicio pendentes={}", pendentes.size());
        int publicadas = 0;
        for (MensagemOutbox mensagem : pendentes) {
            if (!publicar(mensagem)) {
                log.warn("outbox.relay.interrompido sequencia={} eventId={} restantes={}",
                        mensagem.getSequencia(), mensagem.getEventId(), pendentes.size() - publicadas);
                break;
            }
            publicadas++;
        }
        log.info("outbox.relay.fim publicadas={} lote={}", publicadas, pendentes.size());
    }

    private boolean publicar(MensagemOutbox mensagem) {
        try (EscopoMdc escopo = EscopoMdc.deEvento(mensagem.getCorrelationId(), mensagem.getEventId(),
                mensagem.getEventType(), mensagem.getContratoId())) {
            try {
                contextoTrace.executarNoContexto(
                        mensagem.getTraceparent(),
                        "outbox publish " + mensagem.getEventType(),
                        () -> enviar(mensagem));
                mensagem.marcarPublicado();
                log.info("outbox.publicacao.sucesso sequencia={} eventId={} eventType={} contratoId={} topico={} chave={}",
                        mensagem.getSequencia(), mensagem.getEventId(), mensagem.getEventType(),
                        mensagem.getContratoId(), mensagem.getTopico(), mensagem.getChave());
                return true;
            } catch (RuntimeException e) {
                mensagem.registrarFalha(e.getMessage(), maxTentativas);
                log.error("outbox.publicacao.falha sequencia={} eventId={} tentativas={} status={} erro={}",
                        mensagem.getSequencia(), mensagem.getEventId(), mensagem.getTentativas(),
                        mensagem.getStatus(), e.getMessage(), e);
                return false;
            }
        }
    }

    private Object enviar(MensagemOutbox mensagem) {
        ProducerRecord<String, String> registro = new ProducerRecord<>(
                mensagem.getTopico(), null, mensagem.getChave(), mensagem.getPayload());
        adicionarHeaders(registro, mensagem);
        try {
            return kafkaTemplate.send(registro).get(timeoutEnvioMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Publicacao interrompida", e);
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao publicar no Kafka: " + e.getMessage(), e);
        }
    }

    private void adicionarHeaders(ProducerRecord<String, String> registro, MensagemOutbox mensagem) {
        Headers headers = registro.headers();
        HeadersKafka.escrever(headers, EventoHeaders.EVENT_ID, mensagem.getEventId());
        HeadersKafka.escrever(headers, EventoHeaders.EVENT_TYPE, mensagem.getEventType());
        HeadersKafka.escrever(headers, EventoHeaders.EVENT_VERSION, mensagem.getEventVersion());
        HeadersKafka.escrever(headers, EventoHeaders.CONTRATO_ID, mensagem.getContratoId());
        HeadersKafka.escrever(headers, EventoHeaders.CORRELATION_ID, mensagem.getCorrelationId());
        HeadersKafka.escrever(headers, EventoHeaders.OCCURRED_AT, mensagem.getOccurredAt());
        HeadersKafka.escrever(headers, EventoHeaders.PRODUCER, RegistroDeEventosOutbox.PRODUTOR);
        // Com tracing ativo o proprio Spring Kafka injeta o traceparent do span corrente.
        // O header manual cobre o cenario sem tracing, para que o correlacionamento nao dependa dele.
        if (!contextoTrace.tracingAtivo()) {
            HeadersKafka.escrever(headers, EventoHeaders.TRACEPARENT, mensagem.getTraceparent());
        }
    }
}
