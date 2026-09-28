package br.com.freela.contrato.infrastructure.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * Ponte de trace entre a requisicao HTTP e a publicacao assincrona.
 *
 * <p>O problema que isto resolve: o evento e gravado na outbox durante a requisicao HTTP, mas
 * publicado depois, por uma thread do agendador. Sem intervencao, o span do produtor Kafka nasceria
 * solto e o Zipkin mostraria duas operacoes desconexas em vez de um unico trace.</p>
 *
 * <p>A solucao e a propria propagacao W3C: no momento da gravacao, o contexto corrente e serializado
 * em um {@code traceparent} e guardado na linha da outbox; na publicacao, esse {@code traceparent}
 * e reidratado em um span que passa a ser o contexto corrente da thread do relay. A instrumentacao
 * do Spring Kafka entao cria o span de producer como filho dele e injeta os headers na mensagem,
 * de onde os consumidores continuam o mesmo trace.</p>
 *
 * <p>Os {@code ObjectProvider} deixam o componente funcionar tambem quando nao ha tracing
 * configurado, como nos testes de unidade.</p>
 */
@Component
public class ContextoTrace {

    private static final String TRACEPARENT = "traceparent";

    private final ObjectProvider<Tracer> tracerProvider;
    private final ObjectProvider<Propagator> propagatorProvider;

    public ContextoTrace(ObjectProvider<Tracer> tracerProvider, ObjectProvider<Propagator> propagatorProvider) {
        this.tracerProvider = tracerProvider;
        this.propagatorProvider = propagatorProvider;
    }

    /** Serializa o contexto de trace corrente. Retorna {@code null} quando nao ha trace ativo. */
    public String traceparentAtual() {
        Tracer tracer = tracerProvider.getIfAvailable();
        Propagator propagator = propagatorProvider.getIfAvailable();
        if (tracer == null || propagator == null) {
            return null;
        }
        Span atual = tracer.currentSpan();
        if (atual == null) {
            return null;
        }
        Map<String, String> portador = new HashMap<>();
        propagator.inject(atual.context(), portador, Map::put);
        return portador.get(TRACEPARENT);
    }

    /**
     * Executa a acao dentro de um span filho do contexto representado por {@code traceparent}.
     * Sem tracing configurado, ou sem traceparent, apenas executa a acao.
     */
    public <T> T executarNoContexto(String traceparent, String nomeSpan, Supplier<T> acao) {
        Tracer tracer = tracerProvider.getIfAvailable();
        Propagator propagator = propagatorProvider.getIfAvailable();
        if (tracer == null || propagator == null || traceparent == null || traceparent.isBlank()) {
            return acao.get();
        }
        Map<String, String> portador = Map.of(TRACEPARENT, traceparent);
        Span span = propagator.extract(portador, Map::get).name(nomeSpan).start();
        try (Tracer.SpanInScope escopo = tracer.withSpan(span)) {
            return acao.get();
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /** {@code true} quando ha infraestrutura de tracing ativa nesta aplicacao. */
    public boolean tracingAtivo() {
        return tracerProvider.getIfAvailable() != null && propagatorProvider.getIfAvailable() != null;
    }
}
