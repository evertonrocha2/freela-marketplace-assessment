package br.com.freela.observability.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.github.loki4j.logback.json.AbstractJsonProvider;
import com.github.loki4j.logback.json.JsonEventWriter;

/**
 * Acrescenta o campo {@code service} a cada linha JSON.
 *
 * <p>No Loki o servico ja chega como label, mas no arquivo de log nao havia nada dentro da linha
 * dizendo de onde ela veio: so o nome do arquivo. Com o campo, linhas de servicos diferentes podem
 * ser juntadas e filtradas sem perder a origem.</p>
 */
public class ServicoJsonProvider extends AbstractJsonProvider {

    public static final String CAMPO = "service";

    private String servico;

    public void setServico(String servico) {
        this.servico = servico;
    }

    @Override
    public boolean canWrite(ILoggingEvent event) {
        return servico != null && !servico.isBlank();
    }

    @Override
    public boolean writeTo(JsonEventWriter writer, ILoggingEvent event, boolean startWithSeparator) {
        if (startWithSeparator) {
            writer.writeFieldSeparator();
        }
        writer.writeStringField(CAMPO, servico);
        return true;
    }
}
