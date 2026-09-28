package br.com.freela.reputacao;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@ComponentScan(basePackages = {"br.com.freela.reputacao", "br.com.freela.common"})
@EntityScan(basePackages = {"br.com.freela.reputacao", "br.com.freela.common.idempotency"})
@EnableJpaRepositories(basePackages = {"br.com.freela.reputacao", "br.com.freela.common.idempotency"})
public class ReputacaoApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReputacaoApplication.class, args);
    }
}
