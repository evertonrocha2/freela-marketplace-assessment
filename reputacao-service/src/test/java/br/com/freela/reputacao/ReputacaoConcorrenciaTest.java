package br.com.freela.reputacao;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.freela.common.events.ContratoEventoPayload;
import br.com.freela.common.events.EventoEnvelope;
import br.com.freela.common.events.EventoTipos;
import br.com.freela.common.json.EventoJson;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * A chave de particionamento e o contratoId, mas o registro que este servico altera e a reputacao
 * do freelancer. Dois contratos do mesmo freelancer caem em particoes diferentes e sao consumidos
 * por threads diferentes ao mesmo tempo. Este teste reproduz exatamente isso: varias conclusoes
 * simultaneas para o mesmo freelancer, cada uma de um contrato distinto.
 */
@SpringBootTest
class ReputacaoConcorrenciaTest {

    private static final int CONTRATOS = 6;

    @Autowired
    private ReputacaoService service;

    @Autowired
    private ReputacaoRepository repository;

    @Autowired
    private EventoJson json;

    @Test
    @DisplayName("primeiras conclusoes simultaneas de um freelancer novo nao colidem na criacao")
    void freelancerNovoComConclusoesSimultaneas() throws Exception {
        UUID freelancer = UUID.randomUUID();
        BigDecimal valor = new BigDecimal("100.00");

        processarAoMesmoTempo(freelancer, valor);

        ReputacaoFreelancer reputacao = repository.findById(freelancer).orElseThrow();
        assertThat(reputacao.getContratosConcluidos()).isEqualTo(CONTRATOS);
        assertThat(reputacao.getValorTotal()).isEqualByComparingTo(valor.multiply(BigDecimal.valueOf(CONTRATOS)));
    }

    @Test
    @DisplayName("conclusoes simultaneas de um freelancer existente nao perdem incremento")
    void freelancerExistenteNaoPerdeIncremento() throws Exception {
        UUID freelancer = UUID.randomUUID();
        BigDecimal valor = new BigDecimal("100.00");
        service.processar(conclusao(freelancer, valor));

        processarAoMesmoTempo(freelancer, valor);

        ReputacaoFreelancer reputacao = repository.findById(freelancer).orElseThrow();
        assertThat(reputacao.getContratosConcluidos()).isEqualTo(CONTRATOS + 1);
        assertThat(reputacao.getValorTotal())
                .isEqualByComparingTo(valor.multiply(BigDecimal.valueOf(CONTRATOS + 1L)));
    }

    private void processarAoMesmoTempo(UUID freelancer, BigDecimal valor) throws Exception {
        CountDownLatch largada = new CountDownLatch(1);
        ExecutorService threads = Executors.newFixedThreadPool(CONTRATOS);
        try {
            List<Future<?>> tarefas = new ArrayList<>();
            for (int i = 0; i < CONTRATOS; i++) {
                EventoEnvelope evento = conclusao(freelancer, valor);
                tarefas.add(threads.submit(() -> {
                    largada.await();
                    return service.processar(evento);
                }));
            }
            largada.countDown();
            for (Future<?> tarefa : tarefas) {
                tarefa.get(30, TimeUnit.SECONDS);
            }
        } finally {
            threads.shutdownNow();
        }
    }

    private EventoEnvelope conclusao(UUID freelancer, BigDecimal valor) {
        UUID contratoId = UUID.randomUUID();
        var payload = new ContratoEventoPayload(contratoId, UUID.randomUUID(), freelancer,
                "Contrato concorrente", valor, "CONCLUIDO", null);
        return new EventoEnvelope(UUID.randomUUID(), EventoTipos.CONTRATO_CONCLUIDO, EventoEnvelope.VERSAO_ATUAL,
                "Contrato", contratoId, contratoId, Instant.now(), "correlacao-teste", null, "contrato-service",
                json.paraArvore(payload));
    }
}
