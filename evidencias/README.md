# Evidências da execução

Prints de uma execução completa, feita em ordem com a collection do Postman
([postman/Freela-Marketplace.postman_collection.json](../postman/Freela-Marketplace.postman_collection.json)), com o `correlationId`
`print-001`. O contrato principal é o `a504e84d-0ff0-4e2c-9f5e-6371e76ddf81`. Como reproduzir cada
passo está em [docs/EVIDENCIAS.md](../docs/EVIDENCIAS.md).

Nesta máquina, Kafka UI, Zipkin e Loki rodaram nas portas 18090, 19411 e 13100, porque as portas
padrão estavam ocupadas por outro projeto. No docker-compose padrão elas são 8090, 9411 e 3100.

| Item pedido no enunciado | Onde |
|---|---|
| Requisição recebida pelo API Gateway | [1](#1-api-gateway-e-contrato-service) |
| Alteração persistida no contrato-service | [1](#1-api-gateway-e-contrato-service) |
| Evento publicado no Kafka | [1](#1-api-gateway-e-contrato-service) (outbox) e [2](#2-kafka) |
| Consumo do evento pelos serviços interessados | [3](#3-consumo-e-persistência-nos-consumidores) |
| Persistência realizada pelos consumidores | [3](#3-consumo-e-persistência-nos-consumidores) |
| Tratamento de uma mensagem duplicada | [4](#4-mensagem-duplicada) |
| Manutenção da ordem dos eventos de um mesmo contrato | [5](#5-ordem-por-contrato) |
| Logs da mesma operação consultados de forma centralizada | [6](#6-logs-centralizados) |
| Trace correspondente no Zipkin | [7](#7-rastreamento-distribuído) |
| Tratamento de falhas (item 10) | [8](#8-falhas-e-dead-letter-topic) |

---

## 1. API Gateway e contrato-service

Contrato criado pelo gateway: `201 Created`, e o teste confere que o `X-Correlation-Id` volta na
resposta.

![Criar contrato pelo gateway](01-gateway-e-contrato/01-criar-contrato-pelo-gateway.png)

Entrega registrada: `ATIVO` → `ENTREGA_REGISTRADA`.

![Registrar entrega](01-gateway-e-contrato/02-registrar-entrega.png)

Contrato concluído: `ENTREGA_REGISTRADA` → `CONCLUIDO`.

![Concluir contrato](01-gateway-e-contrato/03-concluir-contrato.png)

Estado lido de volta do banco do contrato-service.

![Contrato persistido](01-gateway-e-contrato/04-contrato-persistido-concluido.png)

Outbox: os três eventos gravados na mesma transação da mudança de estado, em ordem, com a chave igual
ao `contratoId` e `status=PUBLICADO` depois do envio ao Kafka.

![Outbox](01-gateway-e-contrato/05-outbox-eventos-publicados.png)

## 2. Kafka

Cluster no ar, com um broker e os tópicos do projeto.

![Cluster Kafka](02-kafka/01-cluster-kafka-online.png)

As mensagens no tópico `freela.contratos.eventos`. As do contrato principal (`a504e84d...`) estão
todas na partição 2, nos offsets 0, 1 e 2, na ordem Criado, Entrega, Concluído. O offset 3 é o
reenvio do teste de duplicidade, com o mesmo `eventId` do offset 2. Os contratos A, B e C caíram
nas partições 0, 1 e 1. A última linha é a mensagem inválida do teste de falha.

![Mensagens no tópico](02-kafka/02-topico-eventos-mensagens.png)

## 3. Consumo e persistência nos consumidores

`notificacao-service`: três notificações, uma por evento. A entrega avisa o cliente e os demais
eventos avisam o freelancer.

![Notificações](03-consumo-e-persistencia/01-notificacoes-registradas.png)

`reputacao-service`: a conclusão contabilizada para o freelancer, com `ultimoEventoId` apontando
para o evento que causou a mudança.

![Reputação](03-consumo-e-persistencia/02-reputacao-atualizada.png)

`auditoria-service`: os três eventos com `eventId`, `eventType`, `contratoId`, `occurredAt`,
`correlationId` e o payload.

![Auditoria](03-consumo-e-persistencia/03-auditoria-registrada.png)

## 4. Mensagem duplicada

O `ContratoConcluido` é republicado com o **mesmo `eventId`**.

![Reenvio](04-mensagem-duplicada/01-reenvio-com-mesmo-eventid.png)

Depois do reenvio, nada muda. Continuam 3 notificações, `contratosConcluidos` continua 1 e
`valorTotal` continua 3500, e a auditoria continua com 3 registros.

![Notificações sem repetição](04-mensagem-duplicada/02-notificacoes-sem-repeticao.png)

![Reputação sem incremento](04-mensagem-duplicada/03-reputacao-sem-incremento.png)

![Auditoria sem duplicata](04-mensagem-duplicada/04-auditoria-sem-duplicata.png)

As marcas de idempotência: uma por `eventId` processado, gravadas no banco de cada consumidor.

![Marcas na notificação](04-mensagem-duplicada/05-marcas-idempotencia-notificacao.png)

![Marcas na reputação](04-mensagem-duplicada/06-marcas-idempotencia-reputacao.png)

## 5. Ordem por contrato

Três contratos com os eventos intercalados: cria A, B e C, depois registra as entregas e depois
conclui os três.

![Criar contrato A](05-ordem-por-contrato/01-criar-contrato-a.png)

![Criar contrato B](05-ordem-por-contrato/02-criar-contrato-b.png)

![Criar contrato C](05-ordem-por-contrato/03-criar-contrato-c.png)

![Registrar entrega A](05-ordem-por-contrato/04-registrar-entrega-a.png)

A auditoria dos três contratos. Os três testes ordenam os eventos pelo momento em que foram
processados (`recebidoEm`) e conferem que, em cada contrato, a sequência foi `ContratoCriado` →
`EntregaRegistrada` → `ContratoConcluido`, mesmo com os eventos intercalados entre contratos e
partições.

![Auditoria: ordem por contrato](05-ordem-por-contrato/05-auditoria-ordem-por-contrato.png)

O freelancer dos três contratos: 3 conclusões e `valorTotal` 300. As conclusões foram consumidas em
partições diferentes, e nenhuma se perdeu.

![Reputação com três conclusões](05-ordem-por-contrato/06-reputacao-tres-conclusoes.png)

## 6. Logs centralizados

No Grafana, o dashboard filtrado pelo `correlationId` `print-001` e pelo `contratoId` mostra as
linhas de todos os serviços, cada uma com `traceId`, `contratoId` e `eventId`. Na tela aparece o
`reputacao-service` descartando o reenvio duplicado (`resultado=DUPLICADO_IGNORADO`).

![Logs no Grafana](06-logs-centralizados/01-grafana-operacao-por-correlationid.png)

A mesma consulta feita direto na API do Loki. O teste confere que ela traz linhas dos cinco
serviços.

![Logs pela API do Loki](06-logs-centralizados/02-loki-consulta-pela-api.png)

## 7. Rastreamento distribuído

Os traces da operação, encontrados no Zipkin pela tag `correlationId`: um por requisição HTTP
(criar, registrar entrega e concluir), todos com raiz no `api-gateway`. O de 13 spans é o da
conclusão. Ele inclui também o reenvio do teste de duplicidade, que reaproveita o contexto de trace
original, e por isso dura 62 s.

![Traces no Zipkin](07-tracing/01-zipkin-traces-por-correlationid.png)

A mesma busca pela API do Zipkin. Os spans de consumidor trazem o `contratoId`, e o teste confere
que o trace passa pelo gateway, pelo contrato-service, pelo Kafka e pelos três consumidores.

![Traces pela API do Zipkin](07-tracing/02-zipkin-consulta-pela-api.png)

## 8. Falhas e dead letter topic

Um JSON inválido publicado direto no tópico principal, para forçar a falha nos consumidores.

![Mensagem inválida](08-falhas-e-dlt/01-publicar-mensagem-invalida.png)

A auditoria registrou uma falha por grupo consumidor (notificação, reputação e auditoria), com
tópico, partição e offset de origem, a exceção e o conteúdo da mensagem.

![Falhas registradas](08-falhas-e-dlt/02-falhas-registradas-por-grupo.png)

Reprocessamento pela API: a mensagem é republicada no tópico principal (`reenviadoEm` preenchido).

![Reprocessar falha](08-falhas-e-dlt/03-reprocessar-falha.png)

Como a mensagem continua inválida, ela falha de novo nos três consumidores e volta ao DLT. As
falhas registradas passam de 3 para 6. Uma mensagem que tivesse falhado por causa transitória
seria processada normalmente nesse reenvio.

![Falhas depois do reprocessamento](08-falhas-e-dlt/04-falhas-depois-do-reprocessamento.png)

O DLT no Kafka UI: três cópias da falha original, uma por consumidor, e três do reprocessamento.
As mensagens continuam no tópico para diagnóstico e reprocessamento.

![Tópico DLT](08-falhas-e-dlt/05-topico-dlt-mensagens.png)
