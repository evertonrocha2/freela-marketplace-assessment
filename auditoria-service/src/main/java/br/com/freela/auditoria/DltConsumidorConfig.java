package br.com.freela.auditoria;

import br.com.freela.common.events.EventoHeaders;
import br.com.freela.common.kafka.HeadersKafka;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Container proprio para o listener do DLT.
 *
 * <p>Os listeners de negocio usam o tratador global, que ao esgotar as tentativas publica a
 * mensagem no DLT. Se o listener do proprio DLT usasse esse mesmo tratador, uma falha ao gravar o
 * registro publicaria a mensagem de volta no DLT, ele a leria de novo, falharia de novo, e assim
 * sem fim.</p>
 *
 * <p>Aqui o tratador retenta algumas vezes e, se ainda assim nao conseguir gravar, apenas registra
 * o erro e segue. Nada se perde com isso: a mensagem continua no topico DLT, que e a fonte, e pode
 * ser lida de novo a partir do offset. O que nao pode acontecer e ela se multiplicar.</p>
 */
@Configuration
public class DltConsumidorConfig {

    public static final String FACTORY = "dltListenerContainerFactory";

    private static final Logger log = LoggerFactory.getLogger(DltConsumidorConfig.class);

    @Bean(FACTORY)
    public ConcurrentKafkaListenerContainerFactory<Object, Object> dltListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory.getObject());
        factory.setCommonErrorHandler(tratadorSemRepublicacao());
        return factory;
    }

    private static DefaultErrorHandler tratadorSemRepublicacao() {
        DefaultErrorHandler handler = new DefaultErrorHandler(
                (registro, excecao) -> log.error(
                        "dlt.registro.nao-gravado topico={} particao={} offset={} eventId={} erro={} acao=mensagem-permanece-no-dlt",
                        registro.topic(), registro.partition(), registro.offset(),
                        HeadersKafka.ler(registro.headers(), EventoHeaders.EVENT_ID), excecao.getMessage(), excecao),
                new FixedBackOff(1000L, 2L));
        handler.setRetryListeners((registro, excecao, tentativa) ->
                log.warn("dlt.registro.retentativa tentativa={} particao={} offset={} erro={}",
                        tentativa, registro.partition(), registro.offset(), excecao.getMessage()));
        return handler;
    }
}
