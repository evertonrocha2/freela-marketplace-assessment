package br.com.freela.contrato.infrastructure.web;

import br.com.freela.contrato.infrastructure.outbox.MensagemOutbox;
import br.com.freela.contrato.infrastructure.outbox.MensagemOutboxRepository;
import br.com.freela.contrato.infrastructure.outbox.StatusOutbox;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Leitura da outbox.
 *
 * <p>Nao faz parte do dominio: existe para tornar o mecanismo verificavel. Permite conferir que o
 * evento foi gravado na mesma transacao do contrato, em que ordem, e se ja foi publicado.</p>
 */
@RestController
@RequestMapping("/api/contratos/outbox")
public class OutboxController {

    private static final Logger log = LoggerFactory.getLogger(OutboxController.class);

    private final MensagemOutboxRepository repository;

    public OutboxController(MensagemOutboxRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public List<ItemOutbox> listar(@RequestParam(required = false) UUID contratoId,
                                   @RequestParam(required = false) String correlationId,
                                   @RequestParam(required = false) StatusOutbox status) {
        log.info("http.outbox.listar contratoId={} correlationId={} status={}", contratoId, correlationId, status);
        List<MensagemOutbox> mensagens;
        if (contratoId != null) {
            mensagens = repository.findByContratoIdOrderBySequenciaAsc(contratoId);
        } else if (correlationId != null) {
            mensagens = repository.buscarPorCorrelationId(correlationId);
        } else if (status != null) {
            mensagens = repository.findByStatusOrderBySequenciaAsc(status);
        } else {
            mensagens = repository.findAll();
        }
        return mensagens.stream().map(ItemOutbox::de).toList();
    }

    /** Recoloca uma mensagem que chegou em ERRO na fila de publicacao. */
    @PostMapping("/{sequencia}/reenvio")
    public ItemOutbox reenviar(@PathVariable Long sequencia) {
        MensagemOutbox mensagem = repository.findById(sequencia)
                .orElseThrow(() -> new IllegalArgumentException("Mensagem de outbox não encontrada: " + sequencia));
        mensagem.reenfileirar();
        repository.save(mensagem);
        log.info("outbox.reenvio.solicitado sequencia={} eventId={} contratoId={}",
                sequencia, mensagem.getEventId(), mensagem.getContratoId());
        return ItemOutbox.de(mensagem);
    }

    public record ItemOutbox(Long sequencia, UUID eventId, String eventType, UUID contratoId, String chave,
                             String topico, StatusOutbox status, int tentativas, String correlationId,
                             String traceparent, Instant occurredAt, Instant criadoEm, Instant publicadoEm,
                             String ultimoErro, String payload) {

        static ItemOutbox de(MensagemOutbox m) {
            return new ItemOutbox(m.getSequencia(), m.getEventId(), m.getEventType(), m.getContratoId(),
                    m.getChave(), m.getTopico(), m.getStatus(), m.getTentativas(), m.getCorrelationId(),
                    m.getTraceparent(), m.getOccurredAt(), m.getCriadoEm(), m.getPublicadoEm(),
                    m.getUltimoErro(), m.getPayload());
        }
    }
}
