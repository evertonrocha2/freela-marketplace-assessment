# Observabilidade: logs, correlação e tracing

Três identificadores, com papéis diferentes e complementares:

| Identificador | Origem | Escopo | Serve para |
|---|---|---|---|
| `correlationId` | API Gateway (ou o cliente) | a operação de negócio inteira | juntar tudo que uma requisição provocou, inclusive o que aconteceu depois da resposta |
| `traceId` | instrumentação do Micrometer | o mesmo fluxo, em forma de spans | ver a cronologia e a duração de cada etapa no Zipkin |
| `eventId` | `contrato-service`, por evento | uma mensagem | seguir uma mensagem específica e enxergar a deduplicação |

`contratoId` atravessa os três e é o que amarra tudo ao domínio.

---

## Correlação

O `correlationId` nasce no `RequestLoggingFilter` do API Gateway: se o cliente mandou
`X-Correlation-Id`, ele é respeitado; se não, um UUID é gerado. O valor vai no request
encaminhado e volta no header da resposta, o que dá ao chamador o identificador exato para
procurar depois.

Daí em diante:

1. `CorrelationIdFilter` (em `freela-common`) coloca o valor no MDC da thread do
   `contrato-service`. Toda linha de log passa a carregá-lo.
2. `ContratoApplicationService` lê o MDC e grava o `correlationId` na linha da outbox.
3. O relay publica o valor no envelope e no header `X-Correlation-Id` da mensagem.
4. Cada consumidor monta o MDC a partir dos headers **antes** de desserializar o corpo
   (`HeadersKafka.mdc`). Se o payload estiver corrompido, a linha de erro ainda sai com
   `correlationId` e `eventId` — que é justamente quando rastrear é mais necessário. Depois de
   ler o envelope, o que faltou nos headers é completado com o que veio no corpo
   (`ConsumidorDeEventos`), o que cobre um produtor que não preencheu os headers.
5. `EscopoMdc` é `AutoCloseable` e restaura o MDC ao final. Sem isso, o `correlationId` de uma
   mensagem vazaria para a próxima processada pela mesma thread do container, que é reaproveitada.

O `correlationId` também é devolvido no corpo das respostas de erro do `contrato-service`, então um
erro reportado por um usuário já vem com a chave de busca.

---

## Logs

### Formato

`logback-spring.xml` de cada aplicação inclui `logback/freela-base.xml`, que vem do módulo
`freela-observability`. Três destinos:

| Appender | Destino | Formato |
|---|---|---|
| `CONSOLE` | terminal | texto, com `traceId`, `spanId`, `correlationId`, `contratoId`, `eventId` |
| `ARQUIVO` | `logs/<servico>.json` | uma linha JSON por evento, com rotação |
| `LOKI_ASYNC` | Loki via HTTP | uma linha JSON por evento |

Toda linha JSON leva o campo `service`, gravado por `ServicoJsonProvider`. No Loki o serviço também
é label, mas no arquivo era o único jeito de saber de onde a linha veio sem olhar o nome do arquivo.
O arquivo termina cada objeto com quebra de linha (`JsonPorLinhaLayout`), então pode ser lido com
`grep` ou `jq -c`, linha a linha.

As linhas escritas durante uma operação carregam `traceId` e `spanId`, inclusive as do gateway.
Em WebFlux uma requisição troca de thread, então o gateway lê o trace do contexto de observação da
própria requisição, e não da thread, e liga `spring.reactor.context-propagation: auto` para o
restante dos logs reativos. Linhas de inicialização, que não pertencem a nenhuma operação, saem sem
`traceId`.

Exemplo do console:

```text
2026-09-28 14:33:13.510 INFO  service=notificacao-service traceId=6abaa4d959aa0e02d6f4ee4f948d36c4
  spanId=e810b6c4bce4d3db correlationId=demo-20260928-143312
  contratoId=d03ef9ff-e195-49e3-aa6e-367ce8ec46d9 eventId=b94e4352-32f7-42a8-a460-dd9c57df8cd5
  thread=notificacao-contratos-0-C-1 logger=b.c.f.notificacao.NotificacaoService
  - notificacao.registro.sucesso notificacaoId=1e9f30b7-... resultado=CRIADA
```

### Convenção de nomes

Cada linha começa com um nome pontilhado estável, que serve de filtro:

| Nome | Onde |
|---|---|
| `gateway.request.inicio` / `.fim` | api-gateway |
| `http.contrato.*` | contrato-service, borda HTTP |
| `contrato.criacao.*`, `contrato.entrega.*`, `contrato.conclusao.*`, `contrato.cancelamento.*` | contrato-service, aplicação |
| `contrato.persistence.*` | contrato-service, persistência |
| `contrato.outbox.registrado` | evento gravado na outbox |
| `outbox.relay.inicio` / `.fim`, `outbox.publicacao.sucesso` / `.falha` | relay |
| `kafka.consumo.inicio` / `.fim` / `.falha` / `.retentativa` / `.dlt` | consumidores |
| `idempotencia.duplicado.descartado` | descarte por idempotência |
| `notificacao.registro.*`, `reputacao.atualizacao.*`, `auditoria.registro.*` | efeito de negócio |
| `dlt.mensagem.registrada` / `.reprocessada` | dead letter |

Todo consumo tem início e fim, e o fim traz `resultado=APLICADO | DUPLICADO_IGNORADO |
IGNORADO_POR_TIPO` e a duração.

### Dados sensíveis

Não há log de corpo de requisição, credencial ou dado pessoal. Identificadores de cliente e
freelancer são UUIDs, não documentos ou e-mails. O payload do evento só aparece em base de dados no
`auditoria-service`, que é o serviço cujo propósito é guardar esse conteúdo.

---

## Centralização (Loki + Grafana)

As aplicações enviam as linhas direto para o Loki pelo appender `loki4j`, por HTTP. Não há agente
coletor: os serviços rodam fora do Docker e um coletor precisaria de bind mount do diretório de
logs, o que traria dependência do sistema de arquivos do host sem ganho nenhum aqui.

Labels: apenas `service` e `level`. Cardinalidade baixa é o que o Loki pede. `correlationId`,
`contratoId`, `eventId`, `traceId` e `spanId` viajam no corpo JSON e são filtrados na consulta.

Se o Loki estiver fora do ar, o appender descarta os lotes depois das retentativas e a aplicação
continua funcionando; console e arquivo seguem recebendo tudo. O endereço pode ser trocado com a
variável `LOKI_URL`.

### Consultas

Grafana em `http://localhost:3000`, sem login. O dashboard **Freela - Rastreamento de operação**
já vem provisionado, com campos para `correlationId`, `contratoId` e `eventId`.

```logql
# a operação inteira, em todos os serviços
{service=~".+"} | json | correlationId = `demo-20260928-143312`

# o ciclo de vida de um contrato
{service=~".+"} | json | contratoId = `d03ef9ff-e195-49e3-aa6e-367ce8ec46d9`

# o trajeto de um evento, incluindo os descartes por duplicidade
{service=~".+"} | json | eventId = `1914174b-06b4-4615-9d40-ca5c1f20f2f7`

# só o que falhou
{service=~".+"} |~ `kafka.consumo.falha|kafka.consumo.dlt|outbox.publicacao.falha`
```

Uma consulta por `correlationId` devolve a operação inteira em ordem cronológica, do
`gateway.request.inicio` até o `kafka.consumo.fim` do último consumidor, sem abrir o console de
nenhuma aplicação. Os identificadores acima são de uma execução de exemplo.

O datasource do Loki tem um *derived field* sobre `traceId`: o campo vira link direto para o trace
no Zipkin.

---

## Tracing distribuído

`spring-boot-starter-zipkin` (Micrometer Tracing + Brave) em todas as seis aplicações.
Amostragem em 100% e propagação W3C (`traceparent`).

```yaml
management:
  tracing:
    sampling.probability: 1.0
    propagation.type: w3c
    export.zipkin.endpoint: ${ZIPKIN_URL:http://localhost:9411/api/v2/spans}
```

O nome da propriedade do endpoint mudou no Spring Boot 4. O antigo
`management.zipkin.tracing.endpoint` foi removido e não é mais lido: com ele, a variável
`ZIPKIN_URL` era ignorada sem aviso, e os spans iam sempre para `localhost:9411`, estivesse o Zipkin
lá ou não.

### Buscar um trace sem saber o traceId

Quem investiga chega com o `correlationId` devolvido pelo gateway, ou com o id do contrato, e não
com o `traceId`. Por isso esses dois valores viram tags dos spans:

| Tag | Onde é gravada |
|---|---|
| `correlationId` | span raiz do gateway (`RequestLoggingFilter`), span HTTP de cada serviço (`CorrelationIdFilter`), span de cada consumo no Kafka (`ConsumidorDeEventos`) |
| `contratoId` | span de cada consumo no Kafka |

No Zipkin, a busca `correlationId=<valor>` devolve os traces da operação, um por requisição HTTP.
Pela API:

```bash
curl -sG http://localhost:9411/api/v2/traces --data-urlencode "annotationQuery=correlationId=<valor>"
```

### Atravessando o Kafka

Instrumentação ligada nos dois lados:

```yaml
spring.kafka.template.observation-enabled: true   # producer
spring.kafka.listener.observation-enabled: true   # consumidores
```

Isso resolve o trecho Kafka, mas não o buraco entre a requisição HTTP e a publicação: o evento é
gravado na outbox durante a requisição e publicado depois, por uma thread do agendador. Sem
intervenção, o span do producer nasceria solto e o Zipkin mostraria duas operações desconexas.

A ponte é feita pela própria propagação W3C, em `ContextoTrace`:

1. Na gravação da outbox, `Propagator.inject` serializa o contexto corrente em um `traceparent`,
   que é guardado na linha.
2. Na publicação, `Propagator.extract` reidrata esse contexto em um span (`outbox publish <tipo>`)
   que passa a ser o contexto corrente da thread do relay.
3. A instrumentação do Spring Kafka cria o span do producer como filho dele e injeta os headers na
   mensagem.
4. Os consumidores continuam o mesmo trace a partir desses headers.

### Resultado

O trace abaixo foi devolvido pelo Zipkin numa execução de exemplo, buscando pela tag
`correlationId`. É a requisição de conclusão do contrato:

```text
traceId = 6abaa4d9d6cde62c24b08f0d0ec28e09

api-gateway          SERVER    http post
api-gateway          CLIENT    http post
contrato-service     SERVER    http post /api/contratos/{id}/conclusao
contrato-service               outbox publish contratoconcluido
contrato-service     PRODUCER  freela.contratos.eventos send
reputacao-service    CONSUMER  freela.contratos.eventos process   grupo=reputacao-service   particao=0 offset=51
notificacao-service  CONSUMER  freela.contratos.eventos process   grupo=notificacao-service particao=0 offset=51
auditoria-service    CONSUMER  freela.contratos.eventos process   grupo=auditoria-service   particao=0 offset=51
contrato-service               outbox publish contratoconcluido
contrato-service     PRODUCER  freela.contratos.eventos send
auditoria-service    CONSUMER  freela.contratos.eventos process   grupo=auditoria-service   particao=0 offset=52
reputacao-service    CONSUMER  freela.contratos.eventos process   grupo=reputacao-service   particao=0 offset=52
notificacao-service  CONSUMER  freela.contratos.eventos process   grupo=notificacao-service particao=0 offset=52
```

Cinco serviços, um único trace, atravessando uma fronteira assíncrona. Os spans de consumidor
trazem grupo, partição e offset como tags.

A segunda metade é a reentrega da mesma mensagem feita pela seção 7 do roteiro, para testar a
duplicidade. Ela entra no Kafka como mensagem nova, no offset 52, mas com o mesmo `eventId`. Cai no
mesmo trace porque a outbox guarda o `traceparent` original, e os três consumidores a recebem e
descartam.

Para chegar a um trace específico, busque pela tag `correlationId` no Zipkin, ou ache a linha no
Grafana pelo `correlationId` e clique no `traceId`.

---

## Componentes de infraestrutura

| Componente | Porta | Para quê |
|---|---:|---|
| Zipkin | 9411 | traces |
| Loki | 3100 | logs centralizados |
| Grafana | 3000 | consulta de logs, dashboard provisionado |
| Kafka UI | 8090 | inspeção de tópicos, partições e offsets |
| Eureka | 8761 | service registry |

Todos sobem com `cd infra && docker compose up -d`. Grafana já vem com os datasources Loki e Zipkin
configurados (`infra/grafana/provisioning`) e sem tela de login, para o ambiente local.
