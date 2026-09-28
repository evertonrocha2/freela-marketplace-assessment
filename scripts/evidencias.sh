#!/usr/bin/env bash
# Roteiro completo de evidencias. A saida vai para a tela e para evidencias/<timestamp>.txt.
# As respostas do Loki e do Zipkin tambem sao gravadas, em evidencias/loki-<timestamp>.json e
# evidencias/zipkin-<timestamp>.json, para que a evidencia nao dependa do ambiente estar no ar.
#
# Pre-requisitos: infraestrutura (infra/docker compose up -d) e as seis aplicacoes no ar.
#
# Variaveis opcionais:
#   GATEWAY          endereco do API Gateway            (padrao http://localhost:8080)
#   KAFKA_CONTAINER  nome do container do Kafka         (padrao freela-kafka)
#   LOKI             endereco do Loki                   (padrao http://localhost:3100)
#   ZIPKIN           endereco do Zipkin                 (padrao http://localhost:9411)
#
# Cobre, nesta ordem:
#   1  requisicao recebida pelo API Gateway
#   2  alteracoes persistidas no contrato-service
#   3  evento gravado na outbox na mesma transacao
#   4  evento publicado no Kafka, com chave e particao
#   5  consumo e persistencia nos tres servicos
#   6  a operacao inteira recuperada por correlationId nos bancos
#   7  mensagem duplicada reentregue sem efeito colateral
#   8  ordem dos eventos de um mesmo contrato, com varios contratos em paralelo
#   9  mensagem invalida indo para o DLT, e o reprocessamento dela
#  10  logs da operacao consultados no Loki, por correlationId, contratoId e eventId
#  11  trace da operacao consultado no Zipkin, pela tag correlationId

set -uo pipefail
cd "$(dirname "$0")/.."

GATEWAY="${GATEWAY:-http://localhost:8080}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-freela-kafka}"
LOKI="${LOKI:-http://localhost:3100}"
ZIPKIN="${ZIPKIN:-http://localhost:9411}"
FREELANCER="22222222-2222-2222-2222-222222222222"
CLIENTE="11111111-1111-1111-1111-111111111111"

mkdir -p evidencias
CARIMBO="$(date +%Y%m%d-%H%M%S)"
ARQUIVO="evidencias/evidencias-${CARIMBO}.txt"
ARQUIVO_LOKI="evidencias/loki-${CARIMBO}.json"
ARQUIVO_ZIPKIN="evidencias/zipkin-${CARIMBO}.json"
CID="demo-${CARIMBO}"
# Inicio da execucao em nanossegundos: limita a consulta ao Loki ao que esta execucao produziu.
INICIO_NS="$(date +%s)000000000"

titulo() {
  echo
  echo "============================================================"
  echo "$1"
  echo "============================================================"
}

# Executa um trecho Python sobre o JSON recebido em stdin, sem quebrar o script em caso de erro.
# -X utf8 e necessario no Windows: sem isso o Python le stdin com a codificacao local e quebra
# em qualquer resposta com acento.
py() { PYTHONIOENCODING=utf-8 python -X utf8 -c "$1" 2>/dev/null || echo "(resposta inesperada)"; }

# Roda um comando dentro do container do Kafka. O bash -lc evita que o Git Bash no Windows
# converta o caminho /opt/kafka/... em um caminho do Windows.
kafka() { docker exec "$KAFKA_CONTAINER" bash -lc "$1" 2>/dev/null; }

contar() { py 'import sys,json;d=json.load(sys.stdin);print(len(d) if isinstance(d,list) else 0)'; }

# So conta o que veio como numero; qualquer outra coisa vira 0.
numero() { case "$1" in ''|*[!0-9]*) echo 0;; *) echo "$1";; esac; }

# O registro no Eureka leva alguns segundos depois que as aplicacoes sobem. Ate la o gateway
# responde 503, entao esperamos ele rotear de verdade antes de comecar.
aguardar_gateway() {
  echo "Aguardando o gateway rotear para os servicos..."
  for i in $(seq 1 60); do
    if curl -fsS -o /dev/null "${GATEWAY}/api/contratos" 2>/dev/null        && curl -fsS -o /dev/null "${GATEWAY}/api/auditoria" 2>/dev/null        && curl -fsS -o /dev/null "${GATEWAY}/api/notificacoes" 2>/dev/null        && curl -fsS -o /dev/null "${GATEWAY}/api/reputacoes" 2>/dev/null; then
      echo "   pronto apos ${i} tentativa(s)"
      return 0
    fi
    sleep 3
  done
  echo "   AVISO: o gateway ainda nao roteia para todos os servicos. Verifique http://localhost:8761" >&2
  return 1
}

executar() {

aguardar_gateway

titulo "1. Requisicao recebida pelo API Gateway (correlationId=${CID})"
echo "POST ${GATEWAY}/api/contratos"
RESPOSTA=$(curl -s -D /tmp/freela-headers.txt -X POST "${GATEWAY}/api/contratos" \
  -H 'Content-Type: application/json' \
  -H "X-Correlation-Id: ${CID}" \
  -d "{\"clienteId\":\"${CLIENTE}\",\"freelancerId\":\"${FREELANCER}\",\"titulo\":\"Construcao de API de pagamentos\",\"valor\":3500.00}")
grep -iE "^HTTP/|^X-Correlation-Id" /tmp/freela-headers.txt
echo "$RESPOSTA" | py 'import sys,json;print(json.dumps(json.load(sys.stdin),indent=2,ensure_ascii=False))'
CONTRATO_ID=$(echo "$RESPOSTA" | py 'import sys,json;print(json.load(sys.stdin)["id"])')
echo "contratoId=${CONTRATO_ID}"

titulo "2. Demais transicoes do mesmo contrato (persistencia no contrato-service)"
for etapa in entregas conclusao; do
  echo "POST /api/contratos/{id}/${etapa}"
  curl -s -X POST "${GATEWAY}/api/contratos/${CONTRATO_ID}/${etapa}" -H "X-Correlation-Id: ${CID}" \
    | py 'import sys,json;d=json.load(sys.stdin);print("   status do contrato:",d.get("status",d))'
done
echo "Estado final lido de volta:"
curl -s "${GATEWAY}/api/contratos/${CONTRATO_ID}" \
  | py 'import sys,json;print(json.dumps(json.load(sys.stdin),indent=2,ensure_ascii=False))'

echo
echo "Aguardando a auditoria registrar os tres eventos..."
for _ in $(seq 1 40); do
  N=$(numero "$(curl -s "${GATEWAY}/api/auditoria?contratoId=${CONTRATO_ID}" | contar)")
  [ "$N" -ge 3 ] && break
  sleep 1
done
echo "   auditoria tem ${N} evento(s) deste contrato"

titulo "3. Outbox transacional do contrato-service"
echo "Cada linha foi gravada na mesma transacao da mudanca de estado. A coluna seq define a ordem."
curl -s "${GATEWAY}/api/contratos/outbox?contratoId=${CONTRATO_ID}" | py '
import sys, json
for m in json.load(sys.stdin):
    print("   seq=%-5s %-20s status=%-10s chave=%s" % (m["sequencia"], m["eventType"], m["status"], m["chave"]))
    print("         eventId=%s publicadoEm=%s" % (m["eventId"], m["publicadoEm"]))
'

titulo "4. Mensagens publicadas no topico Kafka"
echo "Formato: Particao:<n> Offset:<n> <chave> <envelope>"
kafka "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic freela.contratos.eventos --from-beginning --timeout-ms 10000 \
  --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true" \
  | grep "$CONTRATO_ID" | cut -c1-320 || echo "   (nenhuma mensagem encontrada)"

echo
echo "Particoes do topico:"
kafka "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic freela.contratos.eventos"

titulo "5. Consumo e persistencia nos tres servicos"
echo "-- notificacao-service"
curl -s "${GATEWAY}/api/notificacoes?contratoId=${CONTRATO_ID}" | py '
import sys, json
for n in json.load(sys.stdin):
    print("   %-20s destinatario=%s" % (n["tipo"], n["destinatarioId"]))
    print("       %s" % n["mensagem"])
'
echo "-- reputacao-service (freelancer ${FREELANCER})"
curl -s "${GATEWAY}/api/reputacoes/${FREELANCER}" | py '
import sys, json
d = json.load(sys.stdin)
print("   concluidos=%s cancelados=%s valorTotal=%s ultimoEventoId=%s"
      % (d.get("contratosConcluidos"), d.get("contratosCancelados"), d.get("valorTotal"), d.get("ultimoEventoId")))
'
echo "-- auditoria-service"
curl -s "${GATEWAY}/api/auditoria?contratoId=${CONTRATO_ID}" | py '
import sys, json
for e in json.load(sys.stdin):
    print("   %-20s occurredAt=%s" % (e["eventType"], e["occurredAt"]))
    print("       eventId=%s correlationId=%s" % (e["eventId"], e["correlationId"]))
'

titulo "6. Correlacao: a operacao inteira recuperada por correlationId"
echo -n "   auditoria    : "; curl -s "${GATEWAY}/api/auditoria?correlationId=${CID}" | contar
echo -n "   notificacoes : "; curl -s "${GATEWAY}/api/notificacoes?correlationId=${CID}" | contar
echo -n "   outbox       : "; curl -s "${GATEWAY}/api/contratos/outbox?correlationId=${CID}" | contar

titulo "7. Mensagem duplicada: a mesma mensagem publicada de novo"
ANTES_NOTIF=$(curl -s "${GATEWAY}/api/notificacoes?contratoId=${CONTRATO_ID}" | contar)
ANTES_AUD=$(curl -s "${GATEWAY}/api/auditoria?contratoId=${CONTRATO_ID}" | contar)
ANTES_REP=$(curl -s "${GATEWAY}/api/reputacoes/${FREELANCER}" | py 'import sys,json;print(json.load(sys.stdin).get("contratosConcluidos",0))')
echo "Antes : notificacoes=${ANTES_NOTIF} auditoria=${ANTES_AUD} contratosConcluidos=${ANTES_REP}"

SEQ=$(curl -s "${GATEWAY}/api/contratos/outbox?contratoId=${CONTRATO_ID}" | py '
import sys, json
m = [x for x in json.load(sys.stdin) if x["eventType"] == "ContratoConcluido"]
print(m[0]["sequencia"] if m else "")
')
echo "Reenviando a mensagem de sequencia ${SEQ}. O eventId e o mesmo, logo os consumidores"
echo "recebem uma mensagem que ja processaram."
curl -s -X POST "${GATEWAY}/api/contratos/outbox/${SEQ}/reenvio" | py '
import sys, json
m = json.load(sys.stdin)
print("   status=%s eventId=%s" % (m["status"], m["eventId"]))
'
sleep 10

DEPOIS_NOTIF=$(curl -s "${GATEWAY}/api/notificacoes?contratoId=${CONTRATO_ID}" | contar)
DEPOIS_AUD=$(curl -s "${GATEWAY}/api/auditoria?contratoId=${CONTRATO_ID}" | contar)
DEPOIS_REP=$(curl -s "${GATEWAY}/api/reputacoes/${FREELANCER}" | py 'import sys,json;print(json.load(sys.stdin).get("contratosConcluidos",0))')
echo "Depois: notificacoes=${DEPOIS_NOTIF} auditoria=${DEPOIS_AUD} contratosConcluidos=${DEPOIS_REP}"
if [ "$ANTES_NOTIF" = "$DEPOIS_NOTIF" ] && [ "$ANTES_AUD" = "$DEPOIS_AUD" ] && [ "$ANTES_REP" = "$DEPOIS_REP" ]; then
  echo "   RESULTADO: nada mudou. Os tres consumidores descartaram a reentrega."
else
  echo "   RESULTADO: houve alteracao. A idempotencia nao funcionou."
fi

echo
echo "Marcas de idempotencia gravadas em cada banco:"
for svc in notificacoes reputacoes; do
  echo "-- ${svc}"
  curl -s "${GATEWAY}/api/${svc}/eventos-processados?contratoId=${CONTRATO_ID}" | py '
import sys, json
for e in json.load(sys.stdin):
    print("   %-20s consumidor=%-20s eventId=%s" % (e["eventType"], e["consumidor"], e["eventId"]))
'
done

titulo "8. Ordenacao: cinco contratos percorrendo o ciclo completo"
echo "Contratos distintos podem ser processados em paralelo (particoes diferentes);"
echo "os eventos de um mesmo contrato precisam chegar em ordem."
echo "Os cinco contratos sao do mesmo freelancer, novo nesta execucao. A particao e escolhida pelo"
echo "contrato, entao as conclusoes dele sao consumidas em paralelo por threads diferentes e"
echo "disputam o mesmo registro de reputacao."
# Freelancer novo a cada execucao: o ultimo bloco do UUID sao 12 digitos tirados do relogio.
FREELANCER_PARALELO="55555555-5555-5555-5555-$(date +%H%M%S)$(printf '%06d' $((RANDOM % 1000000)))"
IDS=()
for i in 1 2 3 4 5; do
  ID=$(curl -s -X POST "${GATEWAY}/api/contratos" -H 'Content-Type: application/json' \
      -H "X-Correlation-Id: ${CID}-ordem-${i}" \
      -d "{\"clienteId\":\"${CLIENTE}\",\"freelancerId\":\"${FREELANCER_PARALELO}\",\"titulo\":\"Contrato paralelo ${i}\",\"valor\":100.00}" \
      | py 'import sys,json;print(json.load(sys.stdin)["id"])')
  IDS+=("$ID")
done
for ID in "${IDS[@]}"; do
  curl -s -o /dev/null -X POST "${GATEWAY}/api/contratos/${ID}/entregas"
done
# As cinco conclusoes saem ao mesmo tempo. A entrega de cada contrato ja foi registrada acima,
# entao a ordem por contrato continua sendo ContratoCriado, EntregaRegistrada, ContratoConcluido.
for ID in "${IDS[@]}"; do
  curl -s -o /dev/null -X POST "${GATEWAY}/api/contratos/${ID}/conclusao" &
done
wait
sleep 12
for ID in "${IDS[@]}"; do
  echo -n "   ${ID}: "
  curl -s "${GATEWAY}/api/auditoria?contratoId=${ID}" | py '
import sys, json
tipos = [e["eventType"] for e in json.load(sys.stdin)]
esperado = ["ContratoCriado", "EntregaRegistrada", "ContratoConcluido"]
print(" -> ".join(tipos), "  [OK]" if tipos == esperado else "  [FORA DE ORDEM]")
'
done
echo
echo "Particoes usadas por esses contratos (chaves diferentes se espalham):"
kafka "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic freela.contratos.eventos --from-beginning --timeout-ms 10000 \
  --formatter-property print.key=true --formatter-property print.partition=true" \
  | grep -E "$(IFS='|'; echo "${IDS[*]}")" \
  | awk -F'	' '{print "   particao=" substr($1,11) "  contrato=" $2}' | sort -u || true

echo
echo "Reputacao do freelancer ${FREELANCER_PARALELO}, depois das cinco conclusoes simultaneas:"
curl -s "${GATEWAY}/api/reputacoes/${FREELANCER_PARALELO}" | py '
import sys, json
d = json.load(sys.stdin)
c = d.get("contratosConcluidos")
print("   concluidos=%s valorTotal=%s  %s" % (c, d.get("valorTotal"),
      "[OK] nenhum incremento perdido" if c == 5 else "[PERDEU INCREMENTO] esperado 5"))
'

titulo "9. Falha no consumo: mensagem invalida vai para o dead letter topic"
echo "Publicando um JSON quebrado direto no topico principal."
echo "A desserializacao falha, o erro nao e retentavel e a mensagem e movida para o DLT."
kafka "echo 'contrato-invalido:{isto nao e json' | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic freela.contratos.eventos \
  --reader-property parse.key=true --reader-property key.separator=:"
sleep 12

echo
echo "Conteudo do DLT:"
kafka "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic freela.contratos.eventos.dlt --from-beginning --timeout-ms 10000 \
  --formatter-property print.key=true --formatter-property print.partition=true" | cut -c1-200 \
  || echo "   (DLT vazio)"

echo
echo "Falhas registradas pelo auditoria-service (consultaveis e reprocessaveis):"
curl -s "${GATEWAY}/api/auditoria/falhas" | py '
import sys, json
f = json.load(sys.stdin)
print("   %d falha(s) registrada(s)" % len(f))
f.sort(key=lambda x: x["recebidoEm"])
for x in f[-6:]:
    print("   grupo=%-20s topicoOriginal=%s particao=%s offset=%s" % (x.get("grupoConsumidor"), x["topicoOriginal"], x["particaoOriginal"], x["offsetOriginal"]))
    print("       excecao=%s" % x["excecao"])
'

echo
echo "Log de erro correspondente no console dos consumidores:"
grep -h "kafka.consumo.dlt\|kafka.consumo.falha" logs/run-*.out 2>/dev/null | tail -4 | cut -c1-260 \
  || echo "   (nada encontrado em logs/)"

echo
echo "Reprocessamento pela API: a falha mais recente e republicada no topico principal."
FALHAS_ANTES=$(numero "$(curl -s "${GATEWAY}/api/auditoria/falhas" | contar)")
FALHA_ID=$(curl -s "${GATEWAY}/api/auditoria/falhas" | py '
import sys, json
f = [x for x in json.load(sys.stdin) if x.get("reenviadoEm") is None]
f.sort(key=lambda x: x["recebidoEm"])
print(f[-1]["id"] if f else "")
')
case "$FALHA_ID" in
  *-*-*-*-*)
    echo "POST /api/auditoria/falhas/${FALHA_ID}/reprocessar"
    curl -s -X POST "${GATEWAY}/api/auditoria/falhas/${FALHA_ID}/reprocessar" | py '
import sys, json
m = json.load(sys.stdin)
print("   reenviadoEm=%s particaoOriginal=%s offsetOriginal=%s" % (m["reenviadoEm"], m["particaoOriginal"], m["offsetOriginal"]))
'
    sleep 12
    FALHAS_DEPOIS=$(numero "$(curl -s "${GATEWAY}/api/auditoria/falhas" | contar)")
    echo "   falhas registradas: antes=${FALHAS_ANTES} depois=${FALHAS_DEPOIS}"
    echo "   A mensagem voltou ao topico principal e foi lida de novo pelos tres consumidores. Como o"
    echo "   corpo continua invalido, ela falhou outra vez em cada um e voltou ao DLT com um offset"
    echo "   original novo, por isso entrou como falha nova e nao como duplicata. Uma mensagem que"
    echo "   tivesse falhado por causa transitoria seria processada normalmente nesse reenvio."
    ;;
  *)
    echo "   nenhuma falha pendente para reprocessar"
    ;;
esac

titulo "10. Logs centralizados: a mesma operacao consultada no Loki"
echo "Consulta enviada ao Loki, a mesma do dashboard do Grafana:"
echo "   {service=~\".+\"} | json | correlationId=\"${CID}\""
consultar_loki() {
  curl -s -G "${LOKI}/loki/api/v1/query_range" \
    --data-urlencode "query={service=~\".+\"} | json | $1" \
    --data-urlencode "start=${INICIO_NS}" \
    --data-urlencode "limit=2000" \
    --data-urlencode "direction=forward" \
    -o "$2"
}
servicos_no_loki() { py '
import sys, json
r = json.load(sys.stdin)["data"]["result"]
print(len({s["stream"].get("service") for s in r if s["values"]}))
' < "$1"; }

echo "Aguardando a ingestao dos logs dos cinco servicos..."
for _ in $(seq 1 30); do
  consultar_loki "correlationId=\"${CID}\"" "$ARQUIVO_LOKI"
  [ "$(numero "$(servicos_no_loki "$ARQUIVO_LOKI")")" -ge 5 ] && break
  sleep 2
done
py '
import sys, json
r = json.load(sys.stdin)["data"]["result"]
linhas = sorted(((int(ts), s["stream"].get("service", "?"), json.loads(l)) for s in r for ts, l in s["values"]),
                key=lambda x: x[0])
porServico = {}
for _, servico, _ in linhas:
    porServico[servico] = porServico.get(servico, 0) + 1
print("   %d linhas de log de %d servicos, vindas de uma unica consulta:" % (len(linhas), len(porServico)))
for servico in sorted(porServico):
    print("      %-22s %4d linhas" % (servico, porServico[servico]))
print()
print("   Primeiras linhas, em ordem de tempo:")
for _, servico, l in linhas[:14]:
    print("      %-20s traceId=%-32s %s" % (servico, l.get("traceId", "-"), l.get("message", "")[:88]))
' < "$ARQUIVO_LOKI"

EVENT_ID_CONCLUIDO=$(curl -s "${GATEWAY}/api/contratos/outbox?contratoId=${CONTRATO_ID}" | py '
import sys, json
m = [x for x in json.load(sys.stdin) if x["eventType"] == "ContratoConcluido"]
print(m[0]["eventId"] if m else "")
')
echo
echo "A mesma busca, pelos outros dois identificadores:"
for filtro in "contratoId=\"${CONTRATO_ID}\"" "eventId=\"${EVENT_ID_CONCLUIDO}\""; do
  consultar_loki "$filtro" /tmp/freela-loki-filtro.json
  echo -n "   ${filtro}: "
  py '
import sys, json
r = json.load(sys.stdin)["data"]["result"]
c = {}
for s in r:
    c[s["stream"].get("service", "?")] = c.get(s["stream"].get("service", "?"), 0) + len(s["values"])
print(", ".join("%s=%d" % (k, c[k]) for k in sorted(c)) or "nenhuma linha")
' < /tmp/freela-loki-filtro.json
done
echo
echo "Resposta completa do Loki gravada em ${ARQUIVO_LOKI}"

titulo "11. Rastreamento distribuido: o trace da operacao no Zipkin"
echo "Consulta: traces com a tag correlationId=${CID}"
echo "A tag e gravada pelo gateway no span raiz, pelos servicos HTTP e pelos consumidores Kafka."
servicos_no_trace() { py '
import sys, json
t = json.load(sys.stdin)
print(max((len({s.get("localEndpoint", {}).get("serviceName") for s in tr}) for tr in t), default=0))
' < "$1"; }
echo "Aguardando o envio dos spans..."
for _ in $(seq 1 30); do
  curl -s -G "${ZIPKIN}/api/v2/traces" \
    --data-urlencode "annotationQuery=correlationId=${CID}" \
    --data-urlencode "limit=20" \
    --data-urlencode "lookback=3600000" \
    -o "$ARQUIVO_ZIPKIN"
  [ "$(numero "$(servicos_no_trace "$ARQUIVO_ZIPKIN")")" -ge 5 ] && break
  sleep 2
done
py '
import sys, json
traces = json.load(sys.stdin)
print("   %d trace(s) encontrado(s), um por requisicao HTTP da operacao." % len(traces))
print()
for tr in sorted(traces, key=lambda t: min(s.get("timestamp", 0) for s in t)):
    raiz = next((s for s in tr if "parentId" not in s), tr[0])
    servicos = sorted({s.get("localEndpoint", {}).get("serviceName", "?") for s in tr})
    print("   traceId=%s  %2d spans  raiz=%s" % (raiz["traceId"], len(tr), raiz.get("name")))
    print("      servicos: %s" % ", ".join(servicos))
maior = max(traces, key=lambda t: len({s.get("localEndpoint", {}).get("serviceName") for s in t}), default=[])
if maior:
    print()
    print("   Spans do trace com mais servicos, em ordem de tempo:")
    for s in sorted(maior, key=lambda s: s.get("timestamp", 0)):
        tags = s.get("tags", {})
        extra = ""
        if "messaging.kafka.source.partition" in tags:
            extra = " grupo=%s particao=%s offset=%s" % (tags.get("messaging.kafka.consumer.group"),
                    tags.get("messaging.kafka.source.partition"), tags.get("messaging.kafka.message.offset"))
        print("      %-20s %-9s %-45s %6.1f ms%s" % (s.get("localEndpoint", {}).get("serviceName", "?"),
              s.get("kind", "-"), s.get("name", "")[:45], s.get("duration", 0) / 1000.0, extra))
' < "$ARQUIVO_ZIPKIN"
echo
echo "Resposta completa do Zipkin gravada em ${ARQUIVO_ZIPKIN}"

titulo "Resumo"
echo "correlationId principal : ${CID}"
echo "contratoId principal    : ${CONTRATO_ID}"
echo
echo "Arquivos desta execucao:"
echo "  ${ARQUIVO}"
echo "  ${ARQUIVO_LOKI}"
echo "  ${ARQUIVO_ZIPKIN}"
echo
echo "Para conferir ao vivo:"
echo "  Logs centralizados : Grafana, dashboard 'Freela - Rastreamento de operacao'"
echo "                       consulta: {service=~\".+\"} | json | correlationId = \`${CID}\`"
echo "  Traces             : ${ZIPKIN}/zipkin/?annotationQuery=correlationId%3D${CID}"
echo "  Topicos Kafka      : Kafka UI, topico freela.contratos.eventos"

}

executar 2>&1 | tee "$ARQUIVO"
echo
echo "saida gravada em ${ARQUIVO}"
