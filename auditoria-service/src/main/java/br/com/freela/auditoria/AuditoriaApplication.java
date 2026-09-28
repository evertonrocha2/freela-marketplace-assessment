package br.com.freela.auditoria;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@ComponentScan(basePackages = {"br.com.freela.auditoria", "br.com.freela.common"})
@EntityScan(basePackages = {"br.com.freela.auditoria", "br.com.freela.common.idempotency"})
@EnableJpaRepositories(basePackages = {"br.com.freela.auditoria", "br.com.freela.common.idempotency"})
public class AuditoriaApplication {

    public static void main(String[] args) {
        SpringApplication.run(AuditoriaApplication.class, args);
    }
}
