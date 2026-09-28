# Evidências da execução

Roteiro para reproduzir cada evidência pedida no enunciado. Cada seção diz o que executar e o que
o resultado comprova. Os prints de uma execução completa estão em
[evidencias/README.md](../evidencias/README.md).

As mesmas chamadas estão prontas na collection do Postman
[postman/Freela-Marketplace.postman_collection.json](../postman/Freela-Marketplace.postman_collection.json).
Importe no Postman e rode as pastas de 1 a 7: as variáveis são preenchidas pelos próprios testes.

| Item pedido pelo enunciado | Seção | O que deve aparecer |
|---|---|---|
| Requisição recebida pelo API Gateway | 1 | `201 Created` com `X-Correlation-Id` de volta |
| Alteração persistida no contrato-service | 2 e 3 | contrato em `CONCLUIDO`, três linhas na outbox com `status=PUBLICADO` |
| Evento publicado no Kafka | 4 | as mensagens do contrato na mesma partição, com a chave do contrato e offsets crescentes |
| Consumo pelos serviços interessados | 5 | os três consumidores processaram os três eventos |
| Persistência realizada pelos consumidores | 5 | três notificações, reputação atualizada, três registros de auditoria |
| Tratamento de mensagem duplicada | 6 | reentrega com o mesmo `eventId`, números antes e depois idênticos |
| Ordem dos eventos de um mesmo contrato | 7 | vários contratos em partições diferentes, cada um com os eventos em ordem |
| Logs da mesma operação, consultados de forma centralizada | 8 | uma única consulta no Grafana traz as linhas dos cinco serviços |
| Trace correspondente no Zipkin | 9 | o trace com gateway, contrato-service, Kafka e os três consumidores |

A seção 10 cobre o tratamento de falhas: mensagem inválida indo para o DLT, uma falha registrada por
grupo consumidor, e o reprocessamento pela API.

---

## Preparação

```bash
cd infra && docker compose up -d && cd ..
```

Depois suba as seis aplicações como descrito no
[README, em "Execução das aplicações"](../README.md#execução-das-aplicações).

Aguarde o registro no Eureka (`http://localhost:8761`). As primeiras chamadas pelo gateway podem
retornar `503` enquanto o registro não completa; leva cerca de 30 segundos.

Uma variável útil para os comandos seguintes:

```bash
CID="minha-operacao-001"
```

---

## 1. Requisição recebida pelo API Gateway

```bash
curl -i -X POST http://localhost:8080/api/contratos \
  -H 'Content-Type: application/json' \
  -H "X-Correlation-Id: ${CID}" \
  -d '{
        "clienteId": "11111111-1111-1111-1111-111111111111",
        "freelancerId": "22222222-2222-2222-2222-222222222222",
        "titulo": "Construcao de API de pagamentos",
        "valor": 3500.00
      }'
```

**O que comprova:** `201 Created` e o header `X-Correlation-Id` de volta na resposta. No log do
gateway, `gateway.request.inicio` e `gateway.request.fim` com o mesmo valor.

Guarde o `id` retornado:

```bash
CONTRATO=<id-retornado>
```

---

## 2. Alteração persistida no contrato-service

```bash
curl -X POST "http://localhost:8080/api/contratos/${CONTRATO}/entregas"  -H "X-Correlation-Id: ${CID}"
curl -X POST "http://localhost:8080/api/contratos/${CONTRATO}/conclusao" -H "X-Correlation-Id: ${CID}"
curl "http://localhost:8080/api/contratos/${CONTRATO}"
```

**O que comprova:** o contrato termina em `CONCLUIDO`. Nos logs,
`contrato.persistence.save.sucesso` para cada transição.

---

## 3. Evento gravado na outbox, na mesma transação

```bash
curl "http://localhost:8080/api/contratos/outbox?contratoId=${CONTRATO}"
```

**O que comprova:** três linhas, com `sequencia` crescente, `chave` igual ao `contratoId`,
`eventId` distinto e `status=PUBLICADO` com `publicadoEm` preenchido. No log,
`contrato.outbox.registrado` aparece **dentro** da mesma operação que gravou o contrato, antes da
resposta HTTP, e `outbox.publicacao.sucesso` aparece depois, na thread do relay.

---

## 4. Evento publicado no Kafka

```bash
docker exec freela-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic freela.contratos.eventos --from-beginning --timeout-ms 8000 \
  --formatter-property print.key=true \
  --formatter-property print.partition=true \
  --formatter-property print.offset=true
```

**O que comprova:** as mensagens do contrato saem todas com a mesma `Partition` e a mesma chave, em
offsets crescentes. O corpo é o envelope descrito em [EVENTOS.md](EVENTOS.md).

Partições do tópico:

```bash
docker exec freela-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic freela.contratos.eventos
```

Deve mostrar `PartitionCount: 3`. Pela interface: `http://localhost:8090`.

---

## 5. Consumo e persistência em cada serviço

```bash
curl "http://localhost:8080/api/notificacoes?contratoId=${CONTRATO}"
curl "http://localhost:8080/api/reputacoes/22222222-2222-2222-2222-222222222222"
curl "http://localhost:8080/api/auditoria?contratoId=${CONTRATO}"
```

**O que comprova:**

- notificação: três registros, um por evento; o da entrega tem o **cliente** como destinatário e
  os outros dois o freelancer;
- reputação: `contratosConcluidos` incrementado e `valorTotal` somado, com `ultimoEventoId`
  apontando para o evento que produziu o estado;
- auditoria: os três eventos, com `eventId`, `eventType`, `contratoId`, `occurredAt`,
  `correlationId` e o payload.

Nos logs de cada consumidor: `kafka.consumo.inicio` com tópico, partição, offset e chave, e
`kafka.consumo.fim` com `resultado` e duração.

---

## 6. Tratamento de mensagem duplicada

A forma mais direta é usar o reenvio da outbox, que republica a mesma mensagem com o **mesmo
`eventId`**:

```bash
# antes
curl -s "http://localhost:8080/api/notificacoes?contratoId=${CONTRATO}" | jq length
curl -s "http://localhost:8080/api/reputacoes/22222222-2222-2222-2222-222222222222"

# descobrir a sequência do ContratoConcluido e reenviar
curl -s "http://localhost:8080/api/contratos/outbox?contratoId=${CONTRATO}"
curl -X POST "http://localhost:8080/api/contratos/outbox/<sequencia>/reenvio"

# depois (aguarde alguns segundos)
curl -s "http://localhost:8080/api/notificacoes?contratoId=${CONTRATO}" | jq length
curl -s "http://localhost:8080/api/reputacoes/22222222-2222-2222-2222-222222222222"
```

**O que comprova:** os números não mudam. Nenhuma notificação nova, nenhum registro novo de
auditoria, contador de reputação intocado.

Nos logs dos três serviços:

```text
idempotencia.duplicado.descartado consumidor=notificacao-service eventId=fc6409fa-... 
notificacao.evento.duplicado eventId=fc6409fa-... nenhumaNotificacaoCriada=true
reputacao.evento.duplicado eventId=fc6409fa-... contadorInalterado=true
auditoria.evento.duplicado eventId=fc6409fa-... nenhumRegistroCriado=true
kafka.consumo.fim ... resultado=DUPLICADO_IGNORADO
```

As marcas gravadas:

```bash
curl "http://localhost:8080/api/notificacoes/eventos-processados?contratoId=${CONTRATO}"
curl "http://localhost:8080/api/reputacoes/eventos-processados?contratoId=${CONTRATO}"
```

---

## 7. Ordem dos eventos de um mesmo contrato

Crie vários contratos e rode o ciclo completo em todos. Depois, para cada um:

```bash
curl "http://localhost:8080/api/auditoria?contratoId=${CONTRATO}"
```

**O que comprova:** a ordem registrada é sempre
`ContratoCriado` → `EntregaRegistrada` → `ContratoConcluido`, para cada contrato, mesmo com
vários contratos sendo processados ao mesmo tempo.

Complementos:

- no consumo do Kafka (item 4), todas as mensagens de um contrato estão na mesma partição, em
  offsets crescentes;
- contratos diferentes aparecem em partições diferentes, o que é a evidência do processamento
  concorrente;
- nos logs, `thread=notificacao-contratos-<n>-C-1` mostra threads distintas para partições
  distintas.

Usando o **mesmo freelancer** em todos os contratos e enviando as conclusões ao mesmo tempo, dá para
ver também a concorrência no `reputacao-service`. Como a partição é escolhida pelo contrato, as
conclusões são consumidas em paralelo por threads diferentes e disputam o mesmo registro de
reputação. O contador do freelancer tem que subir exatamente o número de contratos concluídos. O
porquê está em [CONFIABILIDADE.md](CONFIABILIDADE.md), seção 2.

Os testes automatizados equivalentes são `OutboxKafkaIntegrationTest.ordemPreservadaPorContrato`,
no `contrato-service`, e `ReputacaoConcorrenciaTest`, no `reputacao-service`.

---

## 8. Logs da mesma operação, consultados de forma centralizada

Abra `http://localhost:3000` (sem login), dashboard **Freela - Rastreamento de operação**, e
preencha o campo `correlationId` com o `${CID}` usado.

Ou direto pela API do Loki:

```bash
curl -sG http://localhost:3100/loki/api/v1/query_range \
  --data-urlencode "query={service=~\".+\"} | json | correlationId = \`${CID}\`" \
  --data-urlencode 'limit=200'
```

**O que comprova:** uma consulta traz a operação inteira, em ordem, dos cinco serviços:

```text
api-gateway          gateway.request.inicio ...
contrato-service     contrato.criacao.inicio ...
contrato-service     contrato.outbox.registrado ...
api-gateway          gateway.request.fim ... status=201 CREATED
contrato-service     outbox.publicacao.sucesso ...
notificacao-service  kafka.consumo.inicio ...
notificacao-service  notificacao.registro.sucesso ...
auditoria-service    auditoria.registro.sucesso ...
reputacao-service    reputacao.atualizacao.sucesso ...
```

Sem abrir o console de nenhuma aplicação. A mesma consulta aceita `contratoId` ou `eventId` no
lugar do `correlationId`. As linhas do mesmo pedido HTTP trazem o mesmo `traceId`.

---

## 9. Trace no Zipkin

Abra `http://localhost:9411` e busque pela tag `correlationId=${CID}`, ou clique no `traceId` a
partir de uma linha de log no Grafana.

Pela API:

```bash
curl -sG http://localhost:9411/api/v2/traces --data-urlencode "annotationQuery=correlationId=${CID}"
```

**O que comprova:** um trace por requisição HTTP da operação, cada um contendo os cinco serviços e
atravessando o Kafka. O da conclusão:

```text
api-gateway          SERVER     http post
api-gateway          CLIENT     http post
contrato-service     SERVER     http post /api/contratos/{id}/conclusao
contrato-service                outbox publish contratoconcluido
contrato-service     PRODUCER   freela.contratos.eventos send
reputacao-service    CONSUMER   freela.contratos.eventos process   grupo=reputacao-service   particao=0 offset=51
notificacao-service  CONSUMER   freela.contratos.eventos process   grupo=notificacao-service particao=0 offset=51
auditoria-service    CONSUMER   freela.contratos.eventos process   grupo=auditoria-service   particao=0 offset=51
```

Os spans de consumidor trazem grupo, partição e offset como tags, além de `correlationId` e
`contratoId`.

---

## 10. Falha de processamento e dead letter topic

Publique uma mensagem inválida direto no tópico:

```bash
docker exec freela-kafka bash -c \
  "echo 'contrato-invalido:{isto nao e json' | /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server localhost:9092 --topic freela.contratos.eventos \
     --reader-property parse.key=true --reader-property key.separator=:"
```

Depois de alguns segundos:

```bash
docker exec freela-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic freela.contratos.eventos.dlt \
  --from-beginning --timeout-ms 8000 --formatter-property print.key=true

curl http://localhost:8080/api/auditoria/falhas
```

**O que comprova:**

- a mensagem aparece no DLT, com a chave original;
- aparece **três vezes**, uma por grupo de consumidores — são três falhas independentes;
- `auditoria_falhas` guarda tópico, partição e offset originais, o grupo que falhou e a exceção;
- os logs trazem `kafka.consumo.falha` e `kafka.consumo.dlt`;
- o consumo do tópico principal continua normalmente para as outras mensagens.

Reprocessamento:

```bash
curl -X POST http://localhost:8080/api/auditoria/falhas/<id>/reprocessar
```

A mensagem volta ao tópico principal e é lida de novo pelos três consumidores. Como a do exemplo
continua inválida, ela falha outra vez em cada um e volta ao DLT com um offset original novo: o
número de falhas registradas sobe três. Uma mensagem que tivesse falhado por causa transitória
seria processada nesse reenvio.

---

## Testes automatizados

```bash
mvn test
```

38 testes. Os que cobrem diretamente os requisitos:

| Requisito | Teste |
|---|---|
| Campos obrigatórios do envelope e do payload | `ContratoDeMensagemTest` (6) |
| Eventos de domínio e transições | `ContratoTest` (8 testes) |
| Formato do envelope, chave de particionamento, traceparent | `RegistroDeEventosOutboxTest` (4) |
| Outbox → Kafka contra um broker real, ordem, particionamento | `OutboxKafkaIntegrationTest` (4, com `@EmbeddedKafka`) |
| Duplicidade não gera notificação repetida | `NotificacaoIdempotenciaTest` (3) |
| Duplicidade não incrementa contador | `ReputacaoIdempotenciaTest` (4) |
| Conclusões simultâneas do mesmo freelancer não perdem incremento | `ReputacaoConcorrenciaTest` (2) |
| Duplicidade não duplica registro de auditoria | `AuditoriaIdempotenciaTest` (3) |
| Falha do DLT: uma por grupo, sem duplicar, sem laço de republicação | `DltListenerTest` (4) |

`OutboxKafkaIntegrationTest` sobe um broker Kafka embutido; os demais usam H2 em memória. Nenhum
deles depende da infraestrutura Docker estar no ar.
