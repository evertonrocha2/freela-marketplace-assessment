package br.com.freela.contrato;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * O componente scan traz do modulo comum apenas o que este servico usa: o filtro de correlationId
 * e a serializacao dos eventos. A idempotencia e a politica de erro de consumo ficam de fora
 * porque o contrato-service so produz.
 *
 * <p>O agendamento e necessario para o relay da outbox.</p>
 */
@SpringBootApplication
@ComponentScan(basePackages = {
        "br.com.freela.contrato",
        "br.com.freela.common.correlation",
        "br.com.freela.common.json"})
@EnableScheduling
public class ContratoApplication {

    public static void main(String[] args) {
        SpringApplication.run(ContratoApplication.class, args);
    }
}
