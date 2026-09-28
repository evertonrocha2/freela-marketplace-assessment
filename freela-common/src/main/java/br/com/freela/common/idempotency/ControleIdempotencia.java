package br.com.freela.common.idempotency;

import br.com.freela.common.events.EventoEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Porta de entrada da idempotencia dos consumidores.
 *
 * <p>Uso esperado: chamar {@link #registrarSeInedito} como primeira linha de um metodo
 * {@code @Transactional} que aplica o efeito de negocio. Se retornar {@code false}, o consumidor
 * apenas registra o descarte e retorna, sem tocar nos dados.</p>
 *
 * <p>Ler antes de gravar e seguro aqui porque o mesmo {@code eventId} sempre cai na mesma particao
 * (a chave e o contratoId) e uma particao e consumida por uma unica thread do grupo. Ainda assim a
 * chave primaria existe como ultima barreira: uma violacao aborta a transacao e a mensagem segue
 * para retentativa em vez de produzir efeito duplicado.</p>
 */
@Service
public class ControleIdempotencia {

    private static final Logger log = LoggerFactory.getLogger(ControleIdempotencia.class);

    private final EventoProcessadoRepository repository;

    public ControleIdempotencia(EventoProcessadoRepository repository) {
        this.repository = repository;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public boolean registrarSeInedito(EventoEnvelope envelope, String consumidor) {
        String chave = EventoProcessado.chave(envelope.eventId(), consumidor);
        if (repository.existsById(chave)) {
            log.info("idempotencia.duplicado.descartado consumidor={} eventId={} eventType={} contratoId={}",
                    consumidor, envelope.eventId(), envelope.eventType(), envelope.contratoId());
            return false;
        }
        repository.save(new EventoProcessado(
                envelope.eventId(), consumidor, envelope.eventType(),
                envelope.contratoId(), envelope.correlationId()));
        log.debug("idempotencia.evento.marcado consumidor={} eventId={}", consumidor, envelope.eventId());
        return true;
    }
}
