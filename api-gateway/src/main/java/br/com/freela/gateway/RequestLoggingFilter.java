package br.com.freela.gateway;

import io.micrometer.common.KeyValue;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.handler.TracingObservationHandler;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.observation.ServerRequestObservationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Origem do correlationId de toda operacao da plataforma.
 *
 * <p>Se o cliente enviou {@code X-Correlation-Id}, ele e respeitado; caso contrario um e gerado
 * aqui. O mesmo valor vai no request encaminhado ao servico de destino e tambem na resposta, o que
 * da ao chamador o identificador exato para procurar depois nos logs centralizados.</p>
 *
 * <p>O correlationId tambem vira tag do span da requisicao. Como esse span e a raiz do trace, a
 * busca {@code correlationId=<valor>} no Zipkin devolve o trace inteiro, com todos os servicos.</p>
 *
 * <p>O MDC e preenchido pontualmente em volta de cada chamada de log. Em WebFlux o processamento
 * de uma requisicao passa por varias threads, entao um MDC preenchido no inicio nao estaria mais
 * la no fim, e pior, poderia estar valendo para outra requisicao. Pelo mesmo motivo o traceId e o
 * spanId sao lidos do contexto de observacao da propria requisicao, e nao da thread corrente.</p>
 */
@Component
public class RequestLoggingFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);
    private static final String HEADER_CORRELATION_ID = "X-Correlation-Id";
    private static final String MDC_CORRELATION_ID = "correlationId";
    private static final String MDC_TRACE_ID = "traceId";
    private static final String MDC_SPAN_ID = "spanId";
    private static final String TAG_CORRELATION_ID = "correlationId";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String correlationId = exchange.getRequest().getHeaders().getFirst(HEADER_CORRELATION_ID);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        final String correlationIdFinal = correlationId;
        long inicio = System.currentTimeMillis();

        Optional<ServerRequestObservationContext> observacao =
                ServerRequestObservationContext.findCurrent(exchange.getAttributes());
        observacao.ifPresent(contexto ->
                contexto.addHighCardinalityKeyValue(KeyValue.of(TAG_CORRELATION_ID, correlationIdFinal)));
        Map<String, String> contextoDeLog = contextoDeLog(correlationIdFinal, observacao);

        var request = exchange.getRequest().mutate()
                .header(HEADER_CORRELATION_ID, correlationIdFinal)
                .build();
        // O header vai na resposta com "set" logo antes do commit: nesse ponto os headers vindos do
        // servico de destino ja foram copiados, entao o valor substitui o deles em vez de duplicar.
        exchange.getResponse().beforeCommit(() -> {
            exchange.getResponse().getHeaders().set(HEADER_CORRELATION_ID, correlationIdFinal);
            return Mono.empty();
        });

        comMdc(contextoDeLog, () -> log.info(
                "gateway.request.inicio correlationId={} method={} path={}",
                correlationIdFinal, request.getMethod(), request.getURI().getPath()));

        return chain.filter(exchange.mutate().request(request).build())
                .doFinally(sinal -> comMdc(contextoDeLog, () -> log.info(
                        "gateway.request.fim correlationId={} method={} path={} status={} durationMs={} signal={}",
                        correlationIdFinal, request.getMethod(), request.getURI().getPath(),
                        exchange.getResponse().getStatusCode(), System.currentTimeMillis() - inicio, sinal)));
    }

    private static Map<String, String> contextoDeLog(String correlationId,
                                                     Optional<ServerRequestObservationContext> observacao) {
        Map<String, String> valores = new HashMap<>();
        valores.put(MDC_CORRELATION_ID, correlationId);
        observacao
                .map(contexto -> contexto.<TracingObservationHandler.TracingContext>get(
                        TracingObservationHandler.TracingContext.class))
                .map(TracingObservationHandler.TracingContext::getSpan)
                .map(Span::context)
                .ifPresent((TraceContext trace) -> {
                    valores.put(MDC_TRACE_ID, trace.traceId());
                    valores.put(MDC_SPAN_ID, trace.spanId());
                });
        return valores;
    }

    private static void comMdc(Map<String, String> valores, Runnable acao) {
        Map<String, String> anteriores = new HashMap<>();
        valores.forEach((chave, valor) -> {
            anteriores.put(chave, MDC.get(chave));
            MDC.put(chave, valor);
        });
        try {
            acao.run();
        } finally {
            anteriores.forEach((chave, anterior) -> {
                if (anterior == null) {
                    MDC.remove(chave);
                } else {
                    MDC.put(chave, anterior);
                }
            });
        }
    }

    @Override
    public int getOrder() {
        return -100;
    }
}
