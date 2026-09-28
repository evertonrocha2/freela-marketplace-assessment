# Especificação das mensagens

Este documento é o contrato de comunicação entre os microsserviços. Mudar qualquer coisa descrita
aqui é mudar uma interface pública: produtor e consumidores dependem deste formato.

---

## Tópicos

| Tópico | Partições | Réplicas | Chave | Produtor | Consumidores |
|---|---:|---:|---|---|---|
| `freela.contratos.eventos` | 3 | 1 | `contratoId` (UUID em texto) | `contrato-service` | `notificacao-service`, `reputacao-service`, `auditoria-service` |
| `freela.contratos.eventos.dlt` | 3 | 1 | mesma chave da mensagem original | qualquer consumidor que esgote as tentativas | `auditoria-service` |

Os tópicos são criados pelo `contrato-service` (`TopicosConfig`) com o número de partições acima.
Isso é intencional: se ficassem por conta da criação automática do broker, nasceriam com uma
partição só e não haveria consumo concorrente.

### Por que um único tópico para todo o ciclo de vida

Todos os quatro eventos de contrato vão para o mesmo tópico. Um tópico por tipo de evento seria
mais granular, mas tornaria impossível garantir a ordem entre eles: `ContratoCriado` e
`ContratoConcluido` estariam em tópicos diferentes, sem nenhuma relação de ordenação. Com um tópico
só e a chave sendo o `contratoId`, todos os eventos de um contrato caem na mesma partição, e o
Kafka garante ordem dentro da partição.

### Grupos de consumidores

| Grupo | Serviço | Concorrência | Tópico |
|---|---|---:|---|
| `notificacao-service` | notificacao-service | 3 | `freela.contratos.eventos` |
| `reputacao-service` | reputacao-service | 3 | `freela.contratos.eventos` |
| `auditoria-service` | auditoria-service | 3 | `freela.contratos.eventos` |
| `auditoria-service-dlt` | auditoria-service | 1 | `freela.contratos.eventos.dlt` |

Grupos distintos: cada serviço recebe uma cópia de todos os eventos e mantém o próprio offset.
Um consumidor parado não atrasa nem faz os outros perderem mensagem.

---

## Envelope

Todas as mensagens do tópico principal usam o mesmo envelope. Ele separa metadados (o que qualquer
consumidor precisa) do payload de negócio (o que só quem age sobre o evento precisa entender).

| Campo | Tipo | Obrigatório | Descrição |
|---|---|:---:|---|
| `eventId` | UUID | sim | Identificador único da mensagem. É a base da idempotência dos consumidores. |
| `eventType` | string | sim | `ContratoCriado`, `EntregaRegistrada`, `ContratoConcluido` ou `ContratoCancelado`. |
| `eventVersion` | inteiro | sim | Versão do schema deste tipo de evento. Hoje sempre `1`. |
| `aggregateType` | string | sim | Agregado de origem. Hoje sempre `Contrato`. |
| `aggregateId` | UUID | sim | Identificador do agregado. Igual ao `contratoId`. |
| `contratoId` | UUID | sim | Contrato relacionado. Também é a chave de particionamento. |
| `occurredAt` | ISO-8601 UTC | sim | Momento em que o fato ocorreu no domínio, não o da publicação. |
| `correlationId` | string | sim | Identificador da operação, originado no API Gateway. |
| `causationId` | string | não | Evento que causou este. Nulo quando a origem foi uma requisição HTTP, o que vale para todos os eventos de hoje: os quatro nascem de um comando HTTP. O campo fica reservado para eventos disparados por outro evento. |
| `producer` | string | sim | Serviço que publicou a mensagem. |
| `payload` | objeto | sim | Dados de negócio. Ver abaixo. |

Notas de formato:

- Datas em ISO-8601 com `Z` (UTC).
- `valor` é decimal e é lido como `BigDecimal` nos consumidores, não como ponto flutuante.
- Campos desconhecidos são ignorados na desserialização, então adicionar campo novo ao envelope
  não quebra consumidor existente. Remover ou renomear campo, sim: isso exige subir `eventVersion`.
- A coluna "Obrigatório" é cobrada pelo código, e não só por este documento. O construtor de
  `EventoEnvelope` rejeita a mensagem sem qualquer campo marcado como obrigatório, e isso vale
  também na desserialização. O consumidor trata essa rejeição como erro não retentável, e a
  mensagem vai direto para o DLT em vez de ser processada pela metade. Coberto por
  `ContratoDeMensagemTest`.

### Payload (`ContratoEventoPayload`)

Os quatro eventos compartilham o mesmo formato de payload. Ele carrega o estado do contrato no
momento do evento, e não apenas o id, para que os consumidores não precisem chamar o
`contrato-service` de volta por HTTP; sem isso, a comunicação seria assíncrona só na aparência.

| Campo | Tipo | Obrigatório | Descrição |
|---|---|:---:|---|
| `contratoId` | UUID | sim | Igual ao do envelope. |
| `clienteId` | UUID | sim | Cliente contratante. |
| `freelancerId` | UUID | sim | Freelancer contratado. |
| `titulo` | string | sim | Título do trabalho. |
| `valor` | decimal | sim | Valor do contrato. |
| `status` | string | sim | Estado do contrato **depois** da transição: `ATIVO`, `ENTREGA_REGISTRADA`, `CONCLUIDO` ou `CANCELADO`. |
| `motivo` | string | não | Preenchido apenas em `ContratoCancelado`. Nulo nos demais. |

Como no envelope, os campos obrigatórios do payload são validados no construtor de
`ContratoEventoPayload`. `motivo` é o único opcional.

---

## Headers Kafka

Os headers duplicam parte do envelope. Existem para permitir filtro, roteamento e diagnóstico sem
desserializar o corpo — em especial quando o corpo é justamente o que está corrompido.

| Header | Conteúdo |
|---|---|
| `eventId` | `eventId` do envelope |
| `eventType` | `eventType` do envelope |
| `eventVersion` | `eventVersion` do envelope |
| `contratoId` | `contratoId` do envelope |
| `X-Correlation-Id` | `correlationId` do envelope |
| `occurredAt` | `occurredAt` do envelope |
| `producer` | serviço produtor |
| `traceparent` | contexto de trace W3C, injetado pela instrumentação do Spring Kafka |

O corpo continua sendo a fonte da verdade. A idempotência dos consumidores lê o `eventId` do
envelope, não do header.

No DLT, o Spring Kafka acrescenta os headers de diagnóstico dele:
`kafka_dlt-original-topic`, `kafka_dlt-original-partition`, `kafka_dlt-original-offset`,
`kafka_dlt-exception-fqcn` e `kafka_dlt-exception-stacktrace`.

---

## Eventos

### ContratoCriado

| | |
|---|---|
| **Tópico** | `freela.contratos.eventos` |
| **Chave** | `contratoId` |
| **Produtor** | `contrato-service` |
| **Consumidores** | `notificacao-service` (cria notificação para o freelancer), `auditoria-service` (registra) |
| **Ignorado por** | `reputacao-service` — criar contrato não altera reputação |
| **Disparado por** | `POST /api/contratos` |
| **Status resultante** | `ATIVO` |

Campos obrigatórios: todos os do envelope, mais `contratoId`, `clienteId`, `freelancerId`,
`titulo`, `valor` e `status` no payload. `motivo` é nulo.

```json
{
  "eventId": "aa30e233-2e88-4c7a-a315-16469a51ee44",
  "eventType": "ContratoCriado",
  "eventVersion": 1,
  "aggregateType": "Contrato",
  "aggregateId": "ecbdf9fc-4101-444b-8184-63d7873f7626",
  "contratoId": "ecbdf9fc-4101-444b-8184-63d7873f7626",
  "occurredAt": "2026-09-21T18:14:31.651566400Z",
  "correlationId": "demo-20260921-151431",
  "causationId": null,
  "producer": "contrato-service",
  "payload": {
    "contratoId": "ecbdf9fc-4101-444b-8184-63d7873f7626",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Construcao de API de pagamentos",
    "valor": 3500.00,
    "status": "ATIVO",
    "motivo": null
  }
}
```

---

### EntregaRegistrada

| | |
|---|---|
| **Tópico** | `freela.contratos.eventos` |
| **Chave** | `contratoId` |
| **Produtor** | `contrato-service` |
| **Consumidores** | `notificacao-service` (notifica o **cliente**), `auditoria-service` |
| **Ignorado por** | `reputacao-service` |
| **Disparado por** | `POST /api/contratos/{id}/entregas` |
| **Status resultante** | `ENTREGA_REGISTRADA` |

Mesmos campos obrigatórios do `ContratoCriado`. É o único evento cuja notificação vai para o
cliente, e não para o freelancer.

```json
{
  "eventId": "3776568c-d0a4-498a-8598-3504a88908e9",
  "eventType": "EntregaRegistrada",
  "eventVersion": 1,
  "aggregateType": "Contrato",
  "aggregateId": "6ae25f5b-01c3-4d45-8780-e5e08b96d6f7",
  "contratoId": "6ae25f5b-01c3-4d45-8780-e5e08b96d6f7",
  "occurredAt": "2026-09-21T18:17:18.213692Z",
  "correlationId": "demo-20260921-151717",
  "causationId": null,
  "producer": "contrato-service",
  "payload": {
    "contratoId": "6ae25f5b-01c3-4d45-8780-e5e08b96d6f7",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Construcao de API de pagamentos",
    "valor": 3500.00,
    "status": "ENTREGA_REGISTRADA",
    "motivo": null
  }
}
```

---

### ContratoConcluido

| | |
|---|---|
| **Tópico** | `freela.contratos.eventos` |
| **Chave** | `contratoId` |
| **Produtor** | `contrato-service` |
| **Consumidores** | `notificacao-service`, `reputacao-service` (incrementa contratos concluídos e soma o valor), `auditoria-service` |
| **Disparado por** | `POST /api/contratos/{id}/conclusao` |
| **Status resultante** | `CONCLUIDO` |

O `reputacao-service` depende de `freelancerId` e `valor` estarem presentes no payload. É o evento
mais sensível a duplicidade: um contador incrementado duas vezes não tem como ser distinguido
depois de dois contratos reais.

```json
{
  "eventId": "fc6409fa-22fd-4b15-8187-4f0432508f4f",
  "eventType": "ContratoConcluido",
  "eventVersion": 1,
  "aggregateType": "Contrato",
  "aggregateId": "6ae25f5b-01c3-4d45-8780-e5e08b96d6f7",
  "contratoId": "6ae25f5b-01c3-4d45-8780-e5e08b96d6f7",
  "occurredAt": "2026-09-21T18:17:18.277419Z",
  "correlationId": "demo-20260921-151717",
  "causationId": null,
  "producer": "contrato-service",
  "payload": {
    "contratoId": "6ae25f5b-01c3-4d45-8780-e5e08b96d6f7",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Construcao de API de pagamentos",
    "valor": 3500.00,
    "status": "CONCLUIDO",
    "motivo": null
  }
}
```

---

### ContratoCancelado

| | |
|---|---|
| **Tópico** | `freela.contratos.eventos` |
| **Chave** | `contratoId` |
| **Produtor** | `contrato-service` |
| **Consumidores** | `notificacao-service`, `reputacao-service` (incrementa cancelados, **não** soma valor), `auditoria-service` |
| **Disparado por** | `POST /api/contratos/{id}/cancelamento` |
| **Status resultante** | `CANCELADO` |

Único evento em que `motivo` vem preenchido, quando o corpo da requisição informa um.

```json
{
  "eventId": "7c1f4a90-0f2b-4a1d-9e33-2d0f6b5c1a44",
  "eventType": "ContratoCancelado",
  "eventVersion": 1,
  "aggregateType": "Contrato",
  "aggregateId": "b2c6e4d1-38a9-4f77-8b21-6c90f3a5e0d2",
  "contratoId": "b2c6e4d1-38a9-4f77-8b21-6c90f3a5e0d2",
  "occurredAt": "2026-09-21T18:22:04.114021Z",
  "correlationId": "demo-20260921-152204",
  "causationId": null,
  "producer": "contrato-service",
  "payload": {
    "contratoId": "b2c6e4d1-38a9-4f77-8b21-6c90f3a5e0d2",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Construcao de API de pagamentos",
    "valor": 3500.00,
    "status": "CANCELADO",
    "motivo": "cliente desistiu do projeto"
  }
}
```

---

## Matriz de interesse

| Evento | notificacao-service | reputacao-service | auditoria-service |
|---|---|---|---|
| `ContratoCriado` | cria notificação (freelancer) | ignora | registra |
| `EntregaRegistrada` | cria notificação (cliente) | ignora | registra |
| `ContratoConcluido` | cria notificação (freelancer) | `contratosConcluidos++`, soma `valor` | registra |
| `ContratoCancelado` | cria notificação (freelancer) | `contratosCancelados++` | registra |

Quando um consumidor ignora um evento, ele registra em log `resultado=IGNORADO_POR_TIPO` e
**não** grava marca de idempotência: o evento não foi processado, apenas descartado por não ser
do interesse dele.

---

## Evolução do schema

- **Adicionar campo opcional**: não quebra nada. `FAIL_ON_UNKNOWN_PROPERTIES` está desligado nos
  consumidores, então versões antigas ignoram o campo novo.
- **Adicionar tipo de evento**: consumidores existentes caem no caso "ignorado por tipo"; a
  auditoria registra automaticamente, porque não filtra.
- **Remover ou renomear campo, ou mudar significado**: incompatível. Publicar com
  `eventVersion: 2` e manter os consumidores tratando as duas versões até todos migrarem.
