# Confiabilidade: publicação transacional, ordem, idempotência e falhas

Quatro garantias, nesta ordem de dependência: o evento não se perde nem aparece sozinho (outbox),
chega na ordem certa (particionamento), não faz efeito duas vezes (idempotência) e, quando nada
funciona, fica guardado (DLT).

---

## 1. Publicação transacional (outbox)

### O problema

Um commit no PostgreSQL e um `send` no Kafka não compartilham transação. Se o `contrato-service`
publicasse dentro do método de negócio, haveria duas janelas de inconsistência:

- falha depois do `send` e antes do commit: evento publicado, contrato não gravado. Os consumidores
  reagem a algo que não aconteceu.
- falha depois do commit e antes do `send`: contrato gravado, nenhum evento. Os consumidores nunca
  sabem.

### A solução

A mudança de estado e o registro do evento vão na **mesma transação do mesmo banco**:

```java
@Transactional
public Contrato criar(CriarContratoCommand cmd) {
    Contrato contrato = Contrato.criar(...);   // o agregado emite ContratoCriado
    Contrato salvo = repository.salvar(contrato);
    registroDeEventos.registrar(contrato.pullDomainEvents(), correlationId);  // grava na outbox
    return salvo;
}
```

`RegistroDeEventosOutbox.registrar` é anotado com `@Transactional(propagation = MANDATORY)`. Isso
não é decoração: se alguém chamar o método fora de transação, o Spring falha na hora, em vez de
deixar passar uma escrita que poderia ser commitada sozinha e quebrar a atomicidade.

O envelope JSON é montado nesse momento e gravado na coluna `payload`. Serializar na escrita, e não
na publicação, faz com que o conteúdo da mensagem seja decidido dentro da transação de negócio: o
que foi commitado é exatamente o que será publicado.

### O relay

`PublicadorOutbox` roda a cada 300 ms (`freela.outbox.intervalo-ms`):

```sql
select * from outbox_eventos
 where status = 'PENDENTE'
 order by sequencia asc
 limit 50
   for update skip locked
```

`SKIP LOCKED` (`jakarta.persistence.lock.timeout = -2` no Hibernate) impede que duas passadas do
relay publiquem a mesma linha. Ele **não** torna seguro rodar várias instâncias do
`contrato-service`: com duas instâncias, a segunda pularia a linha travada pela primeira e poderia
publicar antes um evento de sequência maior do mesmo contrato. A garantia de ordem da seção 2 vale
para uma instância do relay, que é como o projeto roda. Escalar o produtor exigiria eleger um único
relay ativo, ou particionar a outbox pela mesma chave do Kafka.

Cada mensagem é publicada e o relay **espera o ack** antes de passar para a próxima. Depois do ack,
a linha vira `PUBLICADO`.

### Garantia resultante

Entrega **ao menos uma vez**. Se o broker confirmar e o serviço cair antes do commit do
`marcarPublicado`, a mesma mensagem será publicada de novo na próxima passada. É justamente esse
caso que a idempotência dos consumidores cobre.

### Configuração do producer

```yaml
spring.kafka.producer:
  acks: all
  retries: 10
  properties:
    enable.idempotence: true
    max.in.flight.requests.per.connection: 5
```

`enable.idempotence` faz o broker descartar reenvios do mesmo lote em retentativa interna e
preserva a ordem dentro da partição mesmo com várias requisições em voo.

### Mensagem que não publica

Cada falha incrementa `tentativas` e grava `ultimoErro`. Em `freela.outbox.max-tentativas` (5) a
linha vira `ERRO` e deixa de ser lida, para não travar a fila. Ela continua consultável em
`GET /api/contratos/outbox?status=ERRO` e pode voltar para a fila com
`POST /api/contratos/outbox/{sequencia}/reenvio`.

---

## 2. Ordem e concorrência

### A garantia que o Kafka dá

Ordem existe **dentro de uma partição**, não dentro de um tópico. Então a estratégia é fazer
"mesmo contrato" significar "mesma partição".

### Como isso é obtido

**Chave da mensagem = `contratoId`.** O particionador padrão do Kafka aplica hash sobre a chave, de
modo que todos os eventos de um contrato caem sempre na mesma partição, e eventos de contratos
diferentes se espalham pelas três.

**Um consumidor por partição.** `concurrency: 3` em cada `@KafkaListener`, com um tópico de 3
partições. Cada thread fica com uma partição e a processa em sequência. Concorrência maior que o
número de partições só criaria consumidores ociosos.

**Ordem de gravação na outbox.** A coluna `sequencia` é uma identity do banco e o relay lê sempre
`order by sequencia`.

**Ordem de publicação.** O relay publica uma a uma, esperando o ack. Se um envio falha, o lote é
interrompido ali: as mensagens seguintes continuam `PENDENTE` e serão publicadas na próxima
passada, nunca na frente da que falhou.

**Ordem das transições.** As alterações carregam o contrato com `SELECT ... FOR UPDATE`
(`buscarPorIdParaAtualizacao`). Sem isso, duas requisições simultâneas sobre o mesmo contrato
poderiam ler o mesmo estado e gravar eventos cuja ordem na outbox não corresponde à ordem real.

### O que isso permite e o que não permite

| Cenário | Comportamento |
|---|---|
| `ContratoCriado` → `EntregaRegistrada` → `ContratoConcluido` no contrato A | Chegam nessa ordem, sempre. |
| Contratos A, B e C ao mesmo tempo | Processados em paralelo, em partições e threads diferentes. |
| Contratos A e B na mesma partição (colisão de hash) | Serializados entre si. Correto, apenas mais lento. |
| Ordem global entre contratos | **Não é garantida**, e não precisa ser. |

### Consumidor que escreve num registro de outra chave

A partição garante ordem por contrato, mas nem todo consumidor escreve num registro do contrato.
O `reputacao-service` escreve na reputação do **freelancer**, e dois contratos do mesmo freelancer
podem cair em partições diferentes e ser processados ao mesmo tempo, por threads diferentes.

Sem proteção, as duas threads leem o mesmo contador, somam um cada e a última gravação apaga a
outra. O `ReputacaoConcorrenciaTest` reproduz isso: seis conclusões simultâneas de um freelancer
que já tinha uma terminavam com o contador em 2, e não em 7. Com freelancer novo, as threads
colidiam na criação do registro e uma delas falhava com violação de chave.

A correção tem duas partes:

- a reputação é lida com `SELECT ... FOR UPDATE` (`ReputacaoRepository.buscarParaAtualizar`), então
  a segunda thread espera a primeira confirmar e lê o valor já somado;
- a criação do registro de um freelancer novo roda em transação própria (`CriadorDeReputacao`). A
  thread que perde a corrida de inserção recebe a violação de chave ali, relê o registro criado
  pela outra e segue, sem abortar a transação que carrega o efeito do evento e a marca de
  idempotência.

Evidência reproduzível: seção 7 de [EVIDENCIAS.md](EVIDENCIAS.md), com vários contratos do mesmo
freelancer concluídos ao mesmo tempo. O teste automatizado é `ReputacaoConcorrenciaTest`.

---

## 3. Idempotência

### Por que é obrigatório

A entrega é ao menos uma vez. Reentrega acontece em rebalanceamento de grupo, em falha entre o ack
do Kafka e o commit do offset, em queda do relay entre o ack e o `marcarPublicado`, e em
reprocessamento manual do DLT. Nenhum desses casos é excepcional.

### Como funciona

Cada serviço tem no próprio banco a tabela `eventos_processados`:

| Coluna | Conteúdo |
|---|---|
| `id` | `eventId:consumidor` (chave primária) |
| `eventId` | identificador do evento |
| `consumidor` | nome do consumidor lógico |
| `eventType`, `contratoId`, `correlationId` | contexto para consulta |
| `processadoEm` | quando foi aplicado |

O consumidor chama `ControleIdempotencia.registrarSeInedito` como primeira linha do método
`@Transactional` que aplica o efeito:

```java
@Transactional
public ResultadoConsumo processar(EventoEnvelope envelope) {
    if (!interessa(envelope.eventType())) return IGNORADO_POR_TIPO;
    if (!idempotencia.registrarSeInedito(envelope, CONSUMIDOR)) return DUPLICADO_IGNORADO;
    // ... efeito de negócio ...
    return APLICADO;
}
```

**A marca e o efeito vão na mesma transação.** É isso que fecha o ciclo: se a transação completa, a
marca existe e a próxima entrega é descartada; se a transação falha, nem a marca nem o efeito ficam
no banco e a reentrega funciona normalmente.

A chave é `eventId:consumidor`, e não só `eventId`, para que um mesmo serviço possa ter mais de um
consumidor lógico sobre o mesmo tópico no futuro sem que um bloqueie o outro.

### Sobre a corrida entre ler e gravar

Ler antes de gravar é seguro aqui porque o mesmo `eventId` sempre cai na mesma partição e uma
partição é consumida por uma única thread do grupo. Ainda assim a chave primária existe como última
barreira: uma violação aborta a transação e a mensagem segue para retentativa, em vez de produzir
efeito duplicado.

### Efeito por consumidor

| Consumidor | Sem idempotência aconteceria | Com idempotência |
|---|---|---|
| `notificacao-service` | Notificação repetida para o destinatário | Uma notificação por `eventId` |
| `reputacao-service` | Contador incrementado duas vezes, valor somado duas vezes | Contador intocado na reentrega |
| `auditoria-service` | Dois registros do mesmo evento | Um registro, reforçado por unicidade em `eventId` |

O caso da reputação é o mais grave: um contador incrementado duas vezes não tem como ser
distinguido de dois contratos reais depois.

### Como observar

Log de cada descarte:

```text
idempotencia.duplicado.descartado consumidor=reputacao-service eventId=... eventType=ContratoConcluido
reputacao.evento.duplicado eventId=... contratoId=... contadorInalterado=true
kafka.consumo.fim servico=reputacao-service resultado=DUPLICADO_IGNORADO
```

E as marcas gravadas: `GET /api/reputacoes/eventos-processados?contratoId=...`.

---

## 4. Tratamento de falhas

### Retentativa

`DefaultErrorHandler` com backoff exponencial, 3 tentativas a partir de 500 ms
(`KafkaConsumidorConfig`).

A retentativa é **bloqueante**, feita pelo próprio container: a partição fica pausada enquanto o
registro é retentado. A alternativa (`@RetryableTopic`) devolveria a mensagem para o fim de outra
fila e quebraria a ordem dos eventos do contrato. O preço é que uma mensagem problemática segura a
partição dela por cerca de 3,5 segundos (500 ms, 1 s e 2 s de espera). As outras duas partições
seguem normalmente. Os contratos que dividem a partição com a mensagem problemática esperam esse
intervalo e depois continuam.

Erros que **não** são retentados, porque tentar de novo produziria exatamente o mesmo resultado:

- `DeserializationException`
- `JacksonException` (JSON malformado)
- `IllegalArgumentException` (envelope inválido)

Esses vão direto para o DLT.

### Dead letter topic

Esgotadas as tentativas, `DeadLetterPublishingRecoverer` move a mensagem para
`freela.contratos.eventos.dlt`, **na mesma partição de origem**, o que mantém a rastreabilidade de
qual partição produziu a falha. O Spring Kafka acrescenta headers com tópico, partição, offset,
classe da exceção e stacktrace originais.

Como os três serviços consomem em grupos distintos, uma mensagem que falha em todos aparece três
vezes no DLT, uma por grupo. Isso é o comportamento correto: são três falhas independentes. O
header `kafka_dlt-original-consumer-group` diz qual grupo falhou.

### Diagnóstico e reprocessamento

O `auditoria-service` consome o DLT e grava cada falha em `auditoria_falhas`, com o grupo que
falhou em `grupoConsumidor`.

Uma falha é identificada por tópico, partição e offset originais **mais** o grupo consumidor
(restrição `uk_falhas_origem_grupo`). O grupo precisa estar na chave justamente por causa das três
cópias descritas acima. A mesma cópia entregue de novo ao listener do DLT, por rebalanceamento ou
reinício antes do commit, encontra o registro existente e é descartada (`DltListenerTest`).

O listener do DLT tem um container próprio (`DltConsumidorConfig`). Ele não usa o tratador dos
listeners de negócio, porque aquele publica no DLT ao esgotar as tentativas: se gravar o registro
falhasse, a mensagem voltaria para o próprio DLT e seria lida de novo, sem fim. O tratador do DLT
retenta duas vezes e, se ainda falhar, registra o erro e segue. A mensagem continua no tópico DLT,
que é a fonte, e pode ser relida a partir do offset.

```bash
curl http://localhost:8080/api/auditoria/falhas
curl "http://localhost:8080/api/auditoria/falhas?contratoId=<id>"
```

Para reprocessar:

```bash
curl -X POST http://localhost:8080/api/auditoria/falhas/<id>/reprocessar
```

A mensagem volta para o tópico principal com a chave original. Os consumidores que já tinham
processado aquele evento antes da falha o descartam pela idempotência; o que falhou tenta de novo.
É a idempotência que torna o reprocessamento uma operação segura, em vez de algo que exija
intervenção manual serviço a serviço.

O reenvio mantém o `correlationId` original nos headers, então a mensagem reprocessada continua
aparecendo junto da operação original nas consultas por correlação. Ele abre um trace novo no
Zipkin: o reenvio é uma operação nova, iniciada por quem chamou a API.

A mensagem permanece no tópico DLT independentemente disso: o registro em banco existe para dar
consulta e reenvio, não para substituir o Kafka como fonte.

Evidência reproduzível: seção 10 de [EVIDENCIAS.md](EVIDENCIAS.md) publica uma mensagem inválida,
mostra as três falhas registradas, uma por grupo, e reprocessa uma delas pela API.

### Isolamento

Até onde uma falha se propaga:

| Escopo | Efeito de uma mensagem problemática |
|---|---|
| Entre serviços | Nenhum. Grupos distintos, offsets independentes. |
| Entre partições | Nenhum. As outras partições seguem consumindo. |
| Na mesma partição | Os contratos que dividem a partição esperam as retentativas, cerca de 3,5 s. Depois do DLT o offset avança e a fila volta a andar. |
| No produtor (relay da outbox) | A fila da outbox é única e ordenada. Uma linha que não publica interrompe a passada, e as seguintes, de qualquer contrato, esperam. Isso dura até a linha virar `ERRO`, depois de `freela.outbox.max-tentativas` (5) passadas. A partir daí ela deixa de ser lida e o relay volta a andar. |

A espera no produtor é o preço de nunca publicar um evento na frente de um anterior que falhou. Uma
falha de publicação costuma ser do broker, e não de uma mensagem específica, então nesse caso nada
publicaria de qualquer forma.
