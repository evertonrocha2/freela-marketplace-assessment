# Freela Marketplace

Projeto de referência para um marketplace de contratação de freelancers construído com arquitetura de microsserviços em Java e Spring.

A aplicação representa um cenário em que clientes contratam freelancers para a execução de trabalhos. O núcleo do sistema é o gerenciamento dos contratos firmados entre as partes. A partir desse domínio, outros serviços mantêm informações relacionadas a notificações, reputação e auditoria.

## Visão geral

O sistema é composto por oito módulos Maven, seis deles aplicações Spring Boot:

- `eureka-server`: registro e descoberta dos serviços.
- `api-gateway`: ponto de entrada HTTP, origem do `correlationId` de cada operação.
- `contrato-service`: ciclo de vida dos contratos e publicação dos eventos.
- `notificacao-service`: notificações do ciclo de vida do contrato.
- `reputacao-service`: números agregados por freelancer.
- `auditoria-service`: registro de todos os eventos e das mensagens que falharam.
- `freela-common`: contrato de mensageria, correlação e idempotência compartilhados.
- `freela-observability`: configuração de log e tracing comum a todas as aplicações.

A comunicação entre o `contrato-service` e os demais é assíncrona, por Apache Kafka. Não há
chamada HTTP entre eles.

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
                       ┌───────────────────┐        ┌────────────────┐
                       │  contrato-service │───────▶│  contrato_db   │
                       │       :8081       │  uma   │   contratos    │
                       │                   │ trans. │ outbox_eventos │
                       └─────────┬─────────┘        └────────────────┘
                                 │
                       relay da outbox (300 ms)
                                 │
                                 ▼
                  ┌──────────────────────────────┐
                  │  freela.contratos.eventos    │   3 partições
                  │  chave = contratoId          │   Kafka KRaft :9092
                  └───┬───────────┬───────────┬──┘
                      │           │           │      (três grupos distintos)
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

### Documentação

| Documento | Conteúdo |
|---|---|
| [docs/ARQUITETURA.md](docs/ARQUITETURA.md) | Visão geral, módulos, caminho de uma operação, decisões e endpoints |
| [docs/EVENTOS.md](docs/EVENTOS.md) | Especificação das mensagens: tópicos, envelope, payload, headers, exemplos |
| [docs/CONFIABILIDADE.md](docs/CONFIABILIDADE.md) | Outbox transacional, ordenação, idempotência e tratamento de falhas |
| [docs/OBSERVABILIDADE.md](docs/OBSERVABILIDADE.md) | Logs, centralização no Loki, correlação e tracing no Zipkin |
| [docs/EVIDENCIAS.md](docs/EVIDENCIAS.md) | Como reproduzir cada evidência pedida |

### Resumo das escolhas

| Assunto | Escolha |
|---|---|
| Tópico | Um só para todo o ciclo de vida do contrato: `freela.contratos.eventos`, 3 partições |
| Chave de particionamento | `contratoId` — mantém ordem por contrato e permite paralelismo entre contratos |
| Publicação transacional | Outbox no `contrato_db`, com relay agendado |
| Concorrência | 3 consumidores por serviço, um por partição |
| Idempotência | `eventId` em `eventos_processados`, gravado na mesma transação do efeito |
| Falhas | Retentativa bloqueante (3 tentativas, backoff exponencial) e depois DLT |
| Logs centralizados | Loki, alimentado direto pelas aplicações; consulta no Grafana |
| Tracing | Micrometer Tracing + Brave, exportando para o Zipkin, propagação W3C através do Kafka |

## Domínio

O domínio principal está no `contrato-service`.

Um contrato representa o vínculo entre um cliente e um freelancer para a execução de um trabalho. Cada contrato possui:

- identificador;
- cliente;
- freelancer;
- título do trabalho;
- valor;
- status;
- data de criação.

Os estados disponíveis são:

```text
ATIVO
ENTREGA_REGISTRADA
CONCLUIDO
CANCELADO
```

O fluxo de negócio previsto pelo modelo é:

```text
ATIVO
  |
  v
ENTREGA_REGISTRADA
  |
  v
CONCLUIDO
```

Um contrato ativo também pode ser cancelado.

O `contrato-service` utiliza uma organização inspirada em Domain-Driven Design, separando domínio, aplicação e infraestrutura.

```text
contrato-service
└── src/main/java/br/com/freela/contrato
    ├── application
    ├── domain
    │   ├── event
    │   ├── model
    │   ├── repository
    │   └── shared
    └── infrastructure
        ├── persistence
        └── web
```

O Aggregate `Contrato` concentra as regras das mudanças de estado e produz um evento de domínio a
cada transição válida: `ContratoCriado`, `EntregaRegistrada`, `ContratoConcluido` e
`ContratoCancelado`. Cada evento carrega o estado do contrato no momento em que o fato ocorreu, e
não apenas o identificador, para que os consumidores ajam sem precisar chamar o `contrato-service`
de volta.

Os eventos ficam acumulados no agregado e são recolhidos pela camada de aplicação, que os grava na
outbox dentro da mesma transação da mudança de estado. Transições inválidas levantam
`TransicaoInvalidaException` e não produzem evento nenhum.

## Serviços

Todos os caminhos abaixo também respondem pelo API Gateway em `http://localhost:8080`.

### contrato-service

Ciclo de vida dos contratos e publicação dos eventos. Porta `8081`, banco `contrato_db`.

| Método | Caminho | Descrição |
|---|---|---|
| `POST` | `/api/contratos` | Cria o contrato. Emite `ContratoCriado`. |
| `GET` | `/api/contratos` | Lista. |
| `GET` | `/api/contratos/{id}` | Busca. |
| `POST` | `/api/contratos/{id}/entregas` | `ATIVO` → `ENTREGA_REGISTRADA`. Emite `EntregaRegistrada`. |
| `POST` | `/api/contratos/{id}/conclusao` | `ENTREGA_REGISTRADA` → `CONCLUIDO`. Emite `ContratoConcluido`. |
| `POST` | `/api/contratos/{id}/cancelamento` | Cancela. Corpo opcional `{"motivo": "..."}`. Emite `ContratoCancelado`. |
| `GET` | `/api/contratos/outbox` | Inspeciona a outbox. Filtros: `contratoId`, `correlationId`, `status`. |
| `POST` | `/api/contratos/outbox/{sequencia}/reenvio` | Recoloca a mensagem na fila de publicação. |

Exemplo de criação de contrato:

```json
{
  "clienteId": "11111111-1111-1111-1111-111111111111",
  "freelancerId": "22222222-2222-2222-2222-222222222222",
  "titulo": "Construção de API de pagamentos",
  "valor": 3500.00
}
```

Transição inválida retorna `409 Conflict` com código `TRANSICAO_INVALIDA`; contrato inexistente
retorna `404`. As respostas de erro trazem o `correlationId` da operação.

As tabelas do banco são `contratos` e `outbox_eventos`.

### notificacao-service

Registra as notificações do ciclo de vida do contrato. Porta `8082`, banco `notificacao_db`.

Reage a todos os quatro eventos. `EntregaRegistrada` notifica o cliente; os demais, o freelancer.
Cada notificação guarda o `eventId` que a originou, o que torna a deduplicação verificável.

| Método | Caminho | Descrição |
|---|---|---|
| `GET` | `/api/notificacoes` | Filtros: `contratoId`, `correlationId`. |
| `GET` | `/api/notificacoes/eventos-processados` | Marcas de idempotência. Filtro: `contratoId`. |

### reputacao-service

Números agregados por freelancer. Porta `8083`, banco `reputacao_db`.

Reage a `ContratoConcluido` (incrementa `contratosConcluidos` e soma o valor) e a
`ContratoCancelado` (incrementa `contratosCancelados`). Ignora os outros dois.

| Método | Caminho | Descrição |
|---|---|---|
| `GET` | `/api/reputacoes` | Lista. |
| `GET` | `/api/reputacoes/{freelancerId}` | Busca por freelancer. |
| `GET` | `/api/reputacoes/eventos-processados` | Marcas de idempotência. Filtro: `contratoId`. |

### auditoria-service

Registra todos os eventos, sem filtrar por tipo, e também as mensagens que foram para o dead
letter topic. Porta `8084`, banco `auditoria_db`.

Cada registro guarda `eventId`, `eventType`, `eventVersion`, `aggregateType`, `aggregateId`,
`contratoId`, `correlationId`, `producer`, `occurredAt`, momento de recebimento e o payload.

| Método | Caminho | Descrição |
|---|---|---|
| `GET` | `/api/auditoria` | Filtros: `contratoId`, `correlationId`, `eventType`. |
| `GET` | `/api/auditoria/falhas` | Mensagens do DLT. Filtros: `contratoId`, `correlationId`. |
| `POST` | `/api/auditoria/falhas/{id}/reprocessar` | Republica a mensagem no tópico principal. |


## API Gateway

O `api-gateway` é o ponto de entrada HTTP para os microsserviços.

Porta:

```text
8080
```

As rotas configuradas são:

| Caminho | Serviço |
|---|---|
| `/api/contratos/**` | `contrato-service` |
| `/api/notificacoes/**` | `notificacao-service` |
| `/api/reputacoes/**` | `reputacao-service` |
| `/api/auditoria/**` | `auditoria-service` |

O Gateway utiliza Eureka para localizar as instâncias dos serviços.

Também existe suporte ao header:

```text
X-Correlation-Id
```

Quando o header não é enviado pelo cliente, o Gateway gera automaticamente um UUID e o encaminha para o serviço de destino.

## Eureka Server

O Eureka Server mantém o registro das aplicações disponíveis no ambiente.

Porta:

```text
8761
```

Interface web:

```text
http://localhost:8761
```

Os microsserviços utilizam, por padrão:

```text
http://localhost:8761/eureka/
```

como endereço do service registry.

## PostgreSQL

O ambiente utiliza uma única instância PostgreSQL com bancos separados por serviço.

```text
Host:     localhost
Porta:    5432
Usuário:  freela
Senha:    freela
```

Bancos criados na inicialização (`infra/postgres/init-databases.sql`):

| Banco | Tabelas |
|---|---|
| `contrato_db` | `contratos`, `outbox_eventos` |
| `notificacao_db` | `notificacoes`, `eventos_processados` |
| `reputacao_db` | `reputacoes`, `eventos_processados` |
| `auditoria_db` | `auditoria_eventos`, `auditoria_falhas`, `eventos_processados` |

`outbox_eventos` é a outbox transacional do produtor. `eventos_processados` guarda as marcas de
idempotência de cada consumidor. `auditoria_falhas` guarda as mensagens que foram para o dead
letter topic.

Os serviços utilizam Hibernate com `ddl-auto: update` para criação e atualização das tabelas.


## Apache Kafka

O Apache Kafka é executado em modo KRaft, sem ZooKeeper.

Para aplicações executadas diretamente na máquina:

```text
localhost:9092
```

Para aplicações executadas dentro da rede Docker:

```text
kafka:19092
```

O ambiente também inclui o Kafka UI em `http://localhost:8090`.

### Tópicos

| Tópico | Partições | Chave | Produtor | Consumidores |
|---|---:|---|---|---|
| `freela.contratos.eventos` | 3 | `contratoId` | `contrato-service` | `notificacao-service`, `reputacao-service`, `auditoria-service` |
| `freela.contratos.eventos.dlt` | 3 | a mesma da original | qualquer consumidor que esgote as tentativas | `auditoria-service` |

Os tópicos são criados pelo `contrato-service` (`TopicosConfig`), e não pela criação automática do
broker: é isso que garante as 3 partições. Um tópico criado sob demanda nasceria com uma partição
só e não haveria consumo concorrente.

### Eventos

`ContratoCriado`, `EntregaRegistrada`, `ContratoConcluido` e `ContratoCancelado`, todos no mesmo
tópico. O formato completo das mensagens está em [docs/EVENTOS.md](docs/EVENTOS.md).

## Logs

Todos os serviços produzem a mesma linha, em três destinos: console (texto), `logs/<servico>.json`
(uma linha JSON por evento) e Loki (para consulta centralizada).

```text
2026-09-21 15:17:18.352 INFO  service=notificacao-service traceId=6ab174c98e74e4cd0e2430ebc21f1d45
  spanId=0e2430ebc21f1d45 correlationId=demo-20260921-151717
  contratoId=6ae25f5b-01c3-4d45-8780-e5e08b96d6f7 eventId=48d9247f-bc46-46d8-b273-ef19b61faf59
  thread=notificacao-contratos-2-C-1 logger=b.c.f.notificacao.NotificacaoService
  - notificacao.registro.sucesso notificacaoId=7e03238b-... resultado=CRIADA
```

Os campos `traceId`, `correlationId`, `contratoId` e `eventId` vêm do MDC e são o que permite
recuperar uma operação inteira no Grafana sem abrir o console de cada aplicação:

```logql
{service=~".+"} | json | correlationId = `demo-20260921-151717`
```

Informações sensíveis não são registradas. O payload do evento só é persistido pelo
`auditoria-service`, que é o serviço cujo propósito é guardá-lo.

Detalhes, convenção de nomes das linhas e consultas prontas em
[docs/OBSERVABILIDADE.md](docs/OBSERVABILIDADE.md).

## Infraestrutura local

Os serviços de infraestrutura estão definidos em `infra/docker-compose.yml`:
PostgreSQL, Kafka (KRaft), Kafka UI, Zipkin, Loki e Grafana.

Para iniciar o ambiente:

```bash
cd infra
docker compose up -d
```

Para verificar os containers:

```bash
docker compose ps
```

Para encerrar:

```bash
docker compose down
```

Para remover também os dados persistidos:

```bash
docker compose down -v
```

O Grafana já sobe com os datasources Loki e Zipkin configurados e com o dashboard
**Freela - Rastreamento de operação** provisionado, sem tela de login.

Se a porta 5432 já estiver em uso na sua máquina, suba o Postgres em outra porta:

```bash
POSTGRES_PORT=5433 docker compose up -d
```

Nesse caso passe a mesma porta para as aplicações (`DB_PORT=5433` no script de inicialização).

## Execução das aplicações

É necessário Java 21. Se houver um JDK mais antigo no `PATH`, aponte `JAVA_HOME` para o 21.

### Com os scripts

```bash
mvn -DskipTests package
bash scripts/subir-aplicacoes.sh
```

O script sobe as seis aplicações na ordem de dependência, espera cada uma responder em
`/actuator/health`, grava os logs de console em `logs/run-<servico>.out` e os PIDs em `logs/pids`.

Para encerrar:

```bash
bash scripts/parar-aplicacoes.sh
```

Variáveis aceitas pelo script de inicialização: `DB_PORT`, `KAFKA_BOOTSTRAP_SERVERS`, `ZIPKIN_URL`,
`LOKI_URL`, `EUREKA_URL` e `PORTA_GATEWAY`, `PORTA_CONTRATO`, `PORTA_NOTIFICACAO`,
`PORTA_REPUTACAO`, `PORTA_AUDITORIA`, `PORTA_EUREKA`.

```bash
DB_PORT=5433 PORTA_CONTRATO=8181 bash scripts/subir-aplicacoes.sh
```

### Módulo a módulo

Cada aplicação também pode ser iniciada isoladamente, um terminal para cada:

```bash
mvn -pl eureka-server spring-boot:run
mvn -pl api-gateway spring-boot:run
mvn -pl contrato-service spring-boot:run
mvn -pl notificacao-service spring-boot:run
mvn -pl reputacao-service spring-boot:run
mvn -pl auditoria-service spring-boot:run
```

O `eureka-server` precisa subir primeiro. As primeiras chamadas pelo gateway podem retornar `503`
enquanto o registro no Eureka não completa, o que leva cerca de 30 segundos.

## Testes

```bash
mvn test
```

38 testes, nenhum deles dependendo da infraestrutura Docker:

| Módulo | Cobertura |
|---|---|
| `freela-common` | Campos obrigatórios do envelope e do payload cobrados pelo código |
| `contrato-service` | Agregado e eventos de domínio; formato do envelope e chave de particionamento; outbox publicando em um broker Kafka embutido, com verificação de ordem e de particionamento |
| `notificacao-service` | Reprocessamento não gera notificação repetida |
| `reputacao-service` | Reprocessamento não incrementa contador nem soma valor de novo; conclusões simultâneas do mesmo freelancer não perdem incremento |
| `auditoria-service` | Reprocessamento não duplica registro; o DLT registra uma falha por grupo consumidor, não duplica a mesma cópia e não republica no próprio DLT |

`OutboxKafkaIntegrationTest` usa `@EmbeddedKafka`; os demais usam H2 em memória.

## Evidências

O roteiro para reproduzir cada evidência pedida no enunciado, com os comandos e o que cada um
comprova, está em [docs/EVIDENCIAS.md](docs/EVIDENCIAS.md): requisição pelo gateway, persistência,
gravação na outbox, publicação no Kafka, consumo pelos três serviços, reentrega de mensagem
duplicada, ordenação com vários contratos em paralelo, mensagem inválida indo para o dead letter
topic e o reprocessamento dela, e a mesma operação consultada no Grafana e no Zipkin.

## Portas

| Componente | Porta |
|---|---:|
| API Gateway | `8080` |
| contrato-service | `8081` |
| notificacao-service | `8082` |
| reputacao-service | `8083` |
| auditoria-service | `8084` |
| Eureka Server | `8761` |
| Kafka | `9092` |
| Kafka UI | `8090` |
| PostgreSQL | `5432` |
| Grafana | `3000` |
| Loki | `3100` |
| Zipkin | `9411` |

## Teste básico

Com a infraestrutura e as aplicações em execução:

```bash
curl -i -X POST http://localhost:8080/api/contratos   -H 'Content-Type: application/json'   -H 'X-Correlation-Id: teste-contrato-001'   -d '{
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Construção de API de pagamentos",
    "valor": 3500.00
  }'
```

Guardando o `id` retornado, o ciclo completo:

```bash
CONTRATO=<id-retornado>
curl -X POST "http://localhost:8080/api/contratos/${CONTRATO}/entregas"  -H 'X-Correlation-Id: teste-contrato-001'
curl -X POST "http://localhost:8080/api/contratos/${CONTRATO}/conclusao" -H 'X-Correlation-Id: teste-contrato-001'
```

E o efeito nos consumidores:

```bash
curl "http://localhost:8080/api/contratos/outbox?contratoId=${CONTRATO}"
curl "http://localhost:8080/api/notificacoes?contratoId=${CONTRATO}"
curl "http://localhost:8080/api/reputacoes/22222222-2222-2222-2222-222222222222"
curl "http://localhost:8080/api/auditoria?contratoId=${CONTRATO}"
```

A operação inteira no Grafana (`http://localhost:3000`):

```logql
{service=~".+"} | json | correlationId = `teste-contrato-001`
```

E o trace correspondente no Zipkin (`http://localhost:9411`).

## Tecnologias

```text
Java 21
Spring Boot 4.1
Spring Cloud Gateway
Netflix Eureka
Spring Kafka
Spring Data JPA
PostgreSQL 16
Apache Kafka 4 (KRaft)
Micrometer Tracing + Brave + Zipkin
Loki + Grafana
Docker Compose
Maven
JUnit 5, Testcontainers-free (EmbeddedKafka + H2)
```
