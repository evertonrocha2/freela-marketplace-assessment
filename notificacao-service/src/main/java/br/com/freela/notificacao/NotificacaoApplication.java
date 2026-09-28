package br.com.freela.notificacao;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * A tabela de idempotencia mora no modulo comum, por isso o scan de entidades e de repositorios
 * precisa incluir {@code br.com.freela.common.idempotency} alem do pacote do proprio servico.
 */
@SpringBootApplication
@ComponentScan(basePackages = {"br.com.freela.notificacao", "br.com.freela.common"})
@EntityScan(basePackages = {"br.com.freela.notificacao", "br.com.freela.common.idempotency"})
@EnableJpaRepositories(basePackages = {"br.com.freela.notificacao", "br.com.freela.common.idempotency"})
public class NotificacaoApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificacaoApplication.class, args);
    }
}
