package br.com.freela.gateway;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
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
 * <p>O MDC e preenchido pontualmente em volta de cada chamada de log. Em WebFlux o processamento
 * de uma requisicao passa por varias threads, entao um MDC preenchido no inicio nao estaria mais
 * la no fim, e pior, poderia estar valendo para outra requisicao.</p>
 */
@Component
public class RequestLoggingFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);
    private static final String HEADER_CORRELATION_ID = "X-Correlation-Id";
    private static final String MDC_CORRELATION_ID = "correlationId";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String correlationId = exchange.getRequest().getHeaders().getFirst(HEADER_CORRELATION_ID);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        final String correlationIdFinal = correlationId;
        long inicio = System.currentTimeMillis();

        var request = exchange.getRequest().mutate()
                .header(HEADER_CORRELATION_ID, correlationIdFinal)
                .build();
        // O header vai na resposta com "set" logo antes do commit: nesse ponto os headers vindos do
        // servico de destino ja foram copiados, entao o valor substitui o deles em vez de duplicar.
        exchange.getResponse().beforeCommit(() -> {
            exchange.getResponse().getHeaders().set(HEADER_CORRELATION_ID, correlationIdFinal);
            return Mono.empty();
        });

        comCorrelationId(correlationIdFinal, () -> log.info(
                "gateway.request.inicio correlationId={} method={} path={}",
                correlationIdFinal, request.getMethod(), request.getURI().getPath()));

        return chain.filter(exchange.mutate().request(request).build())
                .doFinally(sinal -> comCorrelationId(correlationIdFinal, () -> log.info(
                        "gateway.request.fim correlationId={} method={} path={} status={} durationMs={} signal={}",
                        correlationIdFinal, request.getMethod(), request.getURI().getPath(),
                        exchange.getResponse().getStatusCode(), System.currentTimeMillis() - inicio, sinal)));
    }

    private void comCorrelationId(String correlationId, Runnable acao) {
        String anterior = MDC.get(MDC_CORRELATION_ID);
        MDC.put(MDC_CORRELATION_ID, correlationId);
        try {
            acao.run();
        } finally {
            if (anterior == null) {
                MDC.remove(MDC_CORRELATION_ID);
            } else {
                MDC.put(MDC_CORRELATION_ID, anterior);
            }
        }
    }

    @Override
    public int getOrder() {
        return -100;
    }
}
