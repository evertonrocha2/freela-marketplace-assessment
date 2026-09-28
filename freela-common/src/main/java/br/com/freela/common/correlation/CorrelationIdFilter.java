package br.com.freela.common.correlation;

import br.com.freela.common.events.EventoHeaders;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Coloca o correlationId da requisicao no MDC e como tag do span da requisicao.
 *
 * <p>O API Gateway gera o header quando o cliente nao envia; aqui ele e apenas lido. O fallback de
 * geracao cobre chamadas feitas direto na porta do servico, sem passar pelo gateway.</p>
 *
 * <p>A ordem fica logo depois do filtro de observacao do Spring, que abre o span da requisicao.
 * Por isso o span ja e o corrente quando este filtro roda e a tag vai para ele.</p>
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class CorrelationIdFilter extends OncePerRequestFilter {

    private final MarcadorDeTrace marcadorDeTrace;

    public CorrelationIdFilter(MarcadorDeTrace marcadorDeTrace) {
        this.marcadorDeTrace = marcadorDeTrace;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String correlationId = request.getHeader(EventoHeaders.CORRELATION_ID);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        MDC.put(MdcKeys.CORRELATION_ID, correlationId);
        marcadorDeTrace.marcar(MarcadorDeTrace.TAG_CORRELATION_ID, correlationId);
        response.setHeader(EventoHeaders.CORRELATION_ID, correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(MdcKeys.CORRELATION_ID);
        }
    }
}
