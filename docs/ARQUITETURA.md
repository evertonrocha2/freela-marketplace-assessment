# Arquitetura da solução

## O que mudou em relação ao projeto base

No projeto base o `contrato-service` já produzia eventos de domínio, mas eles morriam em uma linha
de log (`contrato.evento.pendente`). Os outros três serviços tinham entidade, repositório e
service, e nada os chamava.

A entrega conecta as duas pontas por Kafka e acrescenta o que faz essa conexão ser confiável:
publicação transacional, ordem por contrato, idempotência, tratamento de falha, logs centralizados
e rastreamento distribuído.

---

## Visão geral

```text
                    HTTP  (X-Correlation-Id)
                             │
                             ▼
                   ┌───────────────────┐
                   │    API Gateway    │  gera o correlationId
                   │       :8080       │  inicia o trace
                   └─────────┬─────────┘
                             │  lb://contrato-service   (Eureka :8761)
                             ▼
                   ┌───────────────────┐        ┌──────────────┐
                   │  contrato-service │───────▶│ contrato_db  │
                   │       :8081       │  uma   │  contratos   │
                   │                   │ trans. │ outbox_eventos│
                   └─────────┬─────────┘        └──────────────┘
                             │
                   relay da outbox (a cada 300 ms)
                             │
                             ▼
              ┌──────────────────────────────┐
              │  freela.contratos.eventos    │   3 partições
              │  chave = contratoId          │   Kafka KRaft :9092
              └───┬───────────┬───────────┬──┘
                  │           │           │        (três grupos distintos)
       ┌──────────▼──┐  ┌─────▼───────┐  ┌▼──────────────┐
       │ notificacao │  │  reputacao  │  │   auditoria   │
       │    :8082    │  │    :8083    │  │     :8084     │
       └──────┬──────┘  └──────┬──────┘  └───────┬───────┘
              │                │                 │
       notificacao_db     reputacao_db      auditoria_db
                                                  ▲
              falha após as tentativas            │
              ─────────────────────────▶ freela.contratos.eventos.dlt

   Observabilidade (todos os serviços):
     logs em JSON ──▶ Loki :3100 ──▶ Grafana :3000
     spans        ──▶ Zipkin :9411
```

---

## Módulos Maven

| Módulo | Papel |
|---|---|
| `freela-observability` | Configuração de log compartilhada (`logback/freela-base.xml`) e as dependências de tracing e de envio ao Loki. Todas as seis aplicações dependem dele. |
| `freela-common` | Contrato de mensageria (envelope, tipos, tópicos, headers), serialização dos eventos, filtro de correlationId, controle de idempotência e política de erro de consumo. |
| `eureka-server` | Service registry. |
| `api-gateway` | Entrada HTTP, geração do correlationId. |
| `contrato-service` | Domínio, outbox e publicação. |
| `notificacao-service`, `reputacao-service`, `auditoria-service` | Consumidores. |

`freela-common` traz consigo as dependências de Web, JPA e Kafka. Por isso o `api-gateway` e o
`eureka-server` dependem só de `freela-observability`: se dependessem de `freela-common`, o Spring
tentaria configurar um DataSource neles.

O que cada aplicação importa do módulo comum é explícito no `@ComponentScan`. O `contrato-service`,
por exemplo, importa `common.correlation` e `common.json`, mas não `common.idempotency` nem
`common.kafka`, porque só produz.

---

## Caminho de uma operação

Tomando `POST /api/contratos` como exemplo:

1. **API Gateway.** `RequestLoggingFilter` lê o header `X-Correlation-Id` ou gera um UUID, repassa
   no request e devolve na resposta. A instrumentação do Spring inicia o trace.
2. **contrato-service (borda).** `CorrelationIdFilter` coloca o correlationId no MDC da thread.
   A partir daqui toda linha de log do serviço carrega esse campo.
3. **contrato-service (aplicação).** `ContratoApplicationService.criar` abre uma transação,
   constrói o agregado `Contrato` (que emite `ContratoCriado`), persiste o contrato e grava o
   evento na tabela `outbox_eventos`. Os dois `INSERT` estão na mesma transação.
4. **Resposta HTTP.** `201 Created`. Nada foi publicado ainda.
5. **Relay da outbox.** A cada 300 ms o `PublicadorOutbox` lê as mensagens `PENDENTE` em ordem de
   sequência, publica no Kafka e marca como `PUBLICADO`. Antes de publicar, ele reidrata o
   contexto de trace guardado na linha, de modo que o span do producer fique dentro do mesmo trace
   da requisição HTTP.
6. **Consumidores.** Os três grupos recebem a mensagem. Cada um checa o `eventId` contra a tabela
   `eventos_processados` do próprio banco e, se for inédito, aplica o efeito e grava a marca — na
   mesma transação.

Os detalhes de confiabilidade (outbox, ordem, idempotência, falhas) estão em
[CONFIABILIDADE.md](CONFIABILIDADE.md). Os de observabilidade, em
[OBSERVABILIDADE.md](OBSERVABILIDADE.md). O formato das mensagens, em [EVENTOS.md](EVENTOS.md).

---

## Decisões e o porquê

**Um tópico, não um por tipo de evento.** A chave da mensagem é o `contratoId`, então todos os
eventos de um contrato caem na mesma partição e o Kafka garante a ordem entre eles. Com tópicos
separados por tipo, `ContratoCriado` e `ContratoConcluido` não teriam relação de ordem nenhuma.

**Outbox em vez de publicar direto.** Um commit no PostgreSQL e um `send` no Kafka não
compartilham transação. Publicar dentro do método de negócio criaria duas janelas de inconsistência:
falha depois do `send` deixa evento publicado sem dado gravado; falha antes do commit deixa dado
gravado sem evento. Com a outbox, o único commit que importa é o do banco.

**Evento carrega o estado, não só o id.** Os consumidores agem sem chamar o `contrato-service` de
volta. Sem isso, a comunicação seria assíncrona só na aparência: cada consumidor criaria um
acoplamento síncrono no momento de processar.

**Retentativa bloqueante, não tópicos de retry.** `@RetryableTopic` mandaria a mensagem para outra
fila e ela voltaria depois das seguintes, quebrando a ordem. A retentativa do container pausa a
partição e tenta de novo o mesmo registro. O custo é que uma mensagem problemática segura a
partição dela por alguns segundos; as outras duas seguem normalmente.

**Envelope serializado como texto.** O producer usa `StringSerializer` e o JSON é montado no
momento em que o evento entra na outbox. O que foi commitado é exatamente o que será publicado, e o
formato da mensagem não depende da versão das classes Java em nenhuma das pontas.

**Serialização própria para eventos.** `EventoJson` mantém um `ObjectMapper` interno em vez de
publicar um bean. Expor um bean `ObjectMapper` faria o Spring Boot desistir do mapper que ele
configura para o HTTP, acoplando o formato dos eventos ao formato da API REST.

**Lock pessimista no contrato ao alterar.** Duas requisições simultâneas sobre o mesmo contrato
poderiam ler o mesmo estado e gravar eventos cuja ordem na outbox não corresponde à ordem real das
transições. `SELECT ... FOR UPDATE` serializa isso.

---

## Endpoints acrescentados

Todos passam pelo API Gateway em `http://localhost:8080`.

### contrato-service

| Método | Caminho | Descrição |
|---|---|---|
| `POST` | `/api/contratos/{id}/entregas` | Registra a entrega. `ATIVO` → `ENTREGA_REGISTRADA`. |
| `POST` | `/api/contratos/{id}/conclusao` | Conclui. `ENTREGA_REGISTRADA` → `CONCLUIDO`. |
| `POST` | `/api/contratos/{id}/cancelamento` | Cancela. Corpo opcional `{"motivo": "..."}`. |
| `GET` | `/api/contratos/outbox` | Lista a outbox. Filtros: `contratoId`, `correlationId`, `status`. |
| `POST` | `/api/contratos/outbox/{sequencia}/reenvio` | Recoloca a mensagem na fila de publicação. |

Transições inválidas retornam `409 Conflict` com código `TRANSICAO_INVALIDA`; contrato inexistente
retorna `404`. As respostas de erro trazem o `correlationId` da operação.

### notificacao-service e reputacao-service

| Método | Caminho | Descrição |
|---|---|---|
| `GET` | `/api/notificacoes` | Filtros `contratoId`, `correlationId`. |
| `GET` | `/api/notificacoes/eventos-processados` | Marcas de idempotência. Filtro `contratoId`. |
| `GET` | `/api/reputacoes/{freelancerId}` | Reputação de um freelancer. |
| `GET` | `/api/reputacoes/eventos-processados` | Marcas de idempotência. Filtro `contratoId`. |

### auditoria-service

| Método | Caminho | Descrição |
|---|---|---|
| `GET` | `/api/auditoria` | Filtros `contratoId`, `correlationId`, `eventType`. |
| `GET` | `/api/auditoria/falhas` | Mensagens que foram para o DLT. Filtros `contratoId`, `correlationId`. |
| `POST` | `/api/auditoria/falhas/{id}/reprocessar` | Republica a mensagem no tópico principal. |

---

## Esquema de dados acrescentado

| Banco | Tabela | Para quê |
|---|---|---|
| `contrato_db` | `outbox_eventos` | Outbox transacional. PK `sequencia` (identity), `eventId` único. |
| `notificacao_db` | `eventos_processados` | Marcas de idempotência. |
| `notificacao_db` | `notificacoes` | Ganhou `eventId` e `correlationId`. |
| `reputacao_db` | `eventos_processados` | Marcas de idempotência. |
| `reputacao_db` | `reputacoes` | Ganhou `contratosCancelados`, `ultimoEventoId`, `atualizadoEm`. |
| `auditoria_db` | `eventos_processados` | Marcas de idempotência. |
| `auditoria_db` | `auditoria_eventos` | Ganhou `eventVersion`, `aggregateType`, `producer`, `occurredAt` e unicidade em `eventId`. |
| `auditoria_db` | `auditoria_falhas` | Mensagens do DLT, com o erro original. |

O schema continua sendo criado pelo Hibernate (`ddl-auto: update`), como no projeto base.
