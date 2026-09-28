package br.com.freela.contrato.infrastructure.kafka;

import br.com.freela.common.events.KafkaTopicos;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Criacao dos topicos pelo produtor.
 *
 * <p>Declarar os topicos aqui, em vez de deixar o broker cria-los sob demanda, e o que garante o
 * numero de particoes. Um topico criado automaticamente nasceria com uma particao so e o consumo
 * concorrente de contratos distintos simplesmente nao aconteceria.</p>
 *
 * <p>O DLT tem o mesmo numero de particoes que o topico principal porque a mensagem com erro e
 * republicada na particao de origem: isso mantem a rastreabilidade de qual particao produziu a
 * falha.</p>
 */
@Configuration
public class TopicosConfig {

    @Bean
    public NewTopic topicoContratosEventos() {
        return TopicBuilder.name(KafkaTopicos.CONTRATOS_EVENTOS)
                .partitions(KafkaTopicos.PARTICOES)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic topicoContratosEventosDlt() {
        return TopicBuilder.name(KafkaTopicos.CONTRATOS_EVENTOS_DLT)
                .partitions(KafkaTopicos.PARTICOES)
                .replicas(1)
                .build();
    }
}
