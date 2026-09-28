package br.com.freela.common.correlation;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * Grava identificadores de negocio como tags do span corrente.
 *
 * <p>O traceId liga os spans de uma operacao, mas ninguem chega ao Zipkin sabendo o traceId. O que
 * se tem em maos e o correlationId devolvido pelo gateway ou o id do contrato. Com essas tags a
 * busca {@code correlationId=<valor>} ou {@code contratoId=<valor>} no Zipkin encontra o trace.</p>
 *
 * <p>Sem tracing ativo (testes, ou tracing desligado) nao ha span corrente e nada e feito.</p>
 */
@Component
public class MarcadorDeTrace {

    public static final String TAG_CORRELATION_ID = "correlationId";
    public static final String TAG_CONTRATO_ID = "contratoId";

    private final ObjectProvider<Tracer> tracer;

    public MarcadorDeTrace(ObjectProvider<Tracer> tracer) {
        this.tracer = tracer;
    }

    public void marcar(String tag, String valor) {
        if (valor == null || valor.isBlank()) {
            return;
        }
        Tracer atual = tracer.getIfAvailable();
        Span span = atual == null ? null : atual.currentSpan();
        if (span != null) {
            span.tag(tag, valor);
        }
    }
}
