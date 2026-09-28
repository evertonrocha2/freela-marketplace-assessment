# Pontos do assessment: onde cada um foi resolvido

Mapa entre o que foi pedido e o que está no código. Os documentos em `docs/` têm o detalhe.

## Comunicação por mensagens

- [x] **Definir tópicos e contratos de eventos** — `freela-common/.../events/` (`KafkaTopicos`,
      `EventoTipos`, `EventoEnvelope`, `ContratoEventoPayload`, `EventoHeaders`).
      Documentado em [docs/EVENTOS.md](docs/EVENTOS.md).
- [x] **Producer no `contrato-service`** — `infrastructure/outbox/PublicadorOutbox`, alimentado por
      `RegistroDeEventosOutbox`.
- [x] **Consumidores nos serviços interessados** — `ContratoEventosListener` em cada um dos três
      serviços, sobre a base comum `ConsumidorDeEventos`.
- [x] **Por que o fluxo é assíncrono** — [docs/ARQUITETURA.md](docs/ARQUITETURA.md), seção
      "Decisões e o porquê".

## Concorrência e ordenação

- [x] **Key da mensagem** — `contratoId`, em `EventoEnvelope.chaveParticionamento()`. Todos os
      eventos de um contrato caem na mesma partição.
- [x] **Concorrência sem quebrar a ordem** — `concurrency: 3` em cada `@KafkaListener`, com tópico
      de 3 partições: uma thread por partição. Retentativa bloqueante, não por tópico de retry.
      Lock pessimista no contrato ao alterar, para que a ordem na outbox reflita a ordem real.
      [docs/CONFIABILIDADE.md](docs/CONFIABILIDADE.md), seção 2.
- [x] **Consumidor que escreve em registro de outra chave** — a reputação é do freelancer, não do
      contrato, então dois contratos dele são consumidos em paralelo. Leitura com
      `SELECT ... FOR UPDATE` e criação em transação própria (`CriadorDeReputacao`).
      `ReputacaoConcorrenciaTest` e a seção 8 de `scripts/evidencias.sh`.

## Duplicidade

- [x] **`eventId` como identificador idempotente** — `ControleIdempotencia` e a tabela
      `eventos_processados`, no banco de cada consumidor.
- [x] **Demonstração** — `scripts/evidencias.sh`, seção 7; e os testes
      `NotificacaoIdempotenciaTest`, `ReputacaoIdempotenciaTest`, `AuditoriaIdempotenciaTest`.

## Mensagens transacionais

- [x] **Outbox no `contrato-service`** — tabela `outbox_eventos`, entidade `MensagemOutbox`,
      relay `PublicadorOutbox`.
- [x] **Atomicidade entre Aggregate e evento** — a gravação na outbox é
      `@Transactional(propagation = MANDATORY)`, então só roda dentro da transação que salva o
      agregado. [docs/CONFIABILIDADE.md](docs/CONFIABILIDADE.md), seção 1.

## Observabilidade

- [x] **Propagar correlation/trace context pelas mensagens** — `correlationId` no envelope e no
      header; `traceparent` guardado na outbox e reidratado na publicação por `ContextoTrace`.
- [x] **Centralizar logs** — appender loki4j em `logback/freela-base.xml`, Loki e Grafana no
      Docker Compose, dashboard provisionado. Toda linha JSON leva `service`, e o arquivo de log
      tem um objeto por linha.
- [x] **Tracing distribuído e Zipkin** — `spring-boot-starter-zipkin` nas seis aplicações,
      amostragem 100%, propagação W3C, observação ligada no `KafkaTemplate` e nos listeners.
      Endpoint em `management.tracing.export.zipkin.endpoint`, o nome atual no Spring Boot 4.
      `correlationId` e `contratoId` como tags dos spans, para buscar o trace sem saber o traceId.
- [x] **Fluxo completo a partir de um contrato** — `scripts/evidencias.sh` e
      [docs/EVIDENCIAS.md](docs/EVIDENCIAS.md). A execução versionada em `evidencias/` inclui as
      respostas do Loki e do Zipkin.

## Falhas

- [x] **Dead letter topic** — `DeadLetterPublishingRecoverer` na mesma partição de origem.
- [x] **Diagnóstico e reprocessamento** — `auditoria_falhas`, com o grupo que falhou, e
      `POST /api/auditoria/falhas/{id}/reprocessar`.
- [x] **Sem laço e sem duplicata no DLT** — container próprio para o listener do DLT
      (`DltConsumidorConfig`) e identidade da falha por origem mais grupo consumidor.
      `DltListenerTest`.

## Testes

- [x] **Producer/consumer** — `OutboxKafkaIntegrationTest` (broker embutido) e os testes de cada
      consumidor.
- [x] **Duplicidade** — um teste por consumidor.
- [x] **Ordenação** — `OutboxKafkaIntegrationTest.ordemPreservadaPorContrato` e
      `contratosDistintosUsamParticoesDiferentes`.
- [x] **Concorrência no consumidor** — `ReputacaoConcorrenciaTest`.
- [x] **Contrato das mensagens** — `ContratoDeMensagemTest`.
- [x] **DLT** — `DltListenerTest`.
- [x] **Integração com broker** — `@EmbeddedKafka` no `contrato-service`; os consumidores usam H2
      e chamam o serviço diretamente, sem depender de broker.

```bash
mvn test        # 38 testes, sem depender da infraestrutura Docker
```

## Requisitos numerados do enunciado

| # | Requisito | Onde |
|---:|---|---|
| 1 | Comunicação baseada em eventos | [docs/EVENTOS.md](docs/EVENTOS.md), [docs/ARQUITETURA.md](docs/ARQUITETURA.md) |
| 2 | Integração dos serviços | `ContratoEventosListener` + service de cada consumidor |
| 3 | Especificação das mensagens | [docs/EVENTOS.md](docs/EVENTOS.md) |
| 4 | Processamento concorrente e ordenação | [docs/CONFIABILIDADE.md](docs/CONFIABILIDADE.md) §2 |
| 5 | Mensagens duplicadas | [docs/CONFIABILIDADE.md](docs/CONFIABILIDADE.md) §3 |
| 6 | Logs da aplicação | [docs/OBSERVABILIDADE.md](docs/OBSERVABILIDADE.md) |
| 7 | Centralização de logs | Loki + Grafana, [docs/OBSERVABILIDADE.md](docs/OBSERVABILIDADE.md) |
| 8 | Rastreamento distribuído | Zipkin, [docs/OBSERVABILIDADE.md](docs/OBSERVABILIDADE.md) |
| 9 | Correlação das operações | `correlationId` do gateway ao consumidor |
| 10 | Tratamento de falhas | [docs/CONFIABILIDADE.md](docs/CONFIABILIDADE.md) §4, DLT e reprocessamento |
| 11 | Infraestrutura | `infra/docker-compose.yml`. Mantém Postgres, Kafka e Kafka UI da base e acrescenta Zipkin, Loki e Grafana. Eureka e Gateway seguem como aplicações, como vieram na base |
| 12 | Documentação | `README.md` e `docs/` |
| 13 | Evidências | `scripts/evidencias.sh`, [docs/EVIDENCIAS.md](docs/EVIDENCIAS.md), execução versionada em `evidencias/` |
