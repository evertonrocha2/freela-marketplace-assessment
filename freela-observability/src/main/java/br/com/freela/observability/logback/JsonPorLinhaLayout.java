package br.com.freela.observability.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.CoreConstants;
import com.github.loki4j.logback.JsonLayout;

/**
 * O mesmo JSON enviado ao Loki, terminado por quebra de linha.
 *
 * <p>O {@link JsonLayout} do loki4j foi feito para o Loki, que recebe cada evento separado, e por
 * isso nao termina a linha. Gravado em arquivo, ele colava um objeto no outro e o arquivo inteiro
 * virava uma unica linha. Com a quebra, o arquivo fica no formato de um JSON por linha, que da
 * para ler com {@code grep}, {@code jq -c} ou qualquer ferramenta de log.</p>
 */
public class JsonPorLinhaLayout extends JsonLayout {

    @Override
    public String doLayout(ILoggingEvent event) {
        return super.doLayout(event) + CoreConstants.LINE_SEPARATOR;
    }
}
