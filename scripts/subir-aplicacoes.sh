#!/usr/bin/env bash
# Sobe as seis aplicacoes em segundo plano, na ordem de dependencia.
#
# Pre-requisito: a infraestrutura precisa estar no ar (cd infra && docker compose up -d).
#
# Variaveis de ambiente aceitas (todas opcionais):
#   DB_PORT                porta do PostgreSQL no host        (padrao 5432)
#   KAFKA_BOOTSTRAP_SERVERS                                   (padrao localhost:9092)
#   ZIPKIN_URL                                                (padrao http://localhost:9411/api/v2/spans)
#   LOKI_URL                                                  (padrao http://localhost:3100/loki/api/v1/push)
#   PORTA_GATEWAY PORTA_CONTRATO PORTA_NOTIFICACAO PORTA_REPUTACAO PORTA_AUDITORIA PORTA_EUREKA
#
# Saida: logs de console em logs/run-<servico>.out e PIDs em logs/pids.

set -euo pipefail
cd "$(dirname "$0")/.."

# O projeto exige Java 21. Se houver um JDK mais antigo no PATH, aponte JAVA_HOME para o 21.
if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
  JAVA="${JAVA_HOME}/bin/java"
else
  JAVA="java"
fi
VERSAO_JAVA="$("$JAVA" -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)"
if [ "${VERSAO_JAVA:-0}" -lt 21 ] 2>/dev/null; then
  echo "Java 21 ou superior e necessario (encontrado: ${VERSAO_JAVA}). Defina JAVA_HOME." >&2
  exit 1
fi

DB_PORT="${DB_PORT:-5432}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export ZIPKIN_URL="${ZIPKIN_URL:-http://localhost:9411/api/v2/spans}"
export LOKI_URL="${LOKI_URL:-http://localhost:3100/loki/api/v1/push}"

PORTA_EUREKA="${PORTA_EUREKA:-8761}"
PORTA_GATEWAY="${PORTA_GATEWAY:-8080}"
PORTA_CONTRATO="${PORTA_CONTRATO:-8081}"
PORTA_NOTIFICACAO="${PORTA_NOTIFICACAO:-8082}"
PORTA_REPUTACAO="${PORTA_REPUTACAO:-8083}"
PORTA_AUDITORIA="${PORTA_AUDITORIA:-8084}"

export EUREKA_URL="${EUREKA_URL:-http://localhost:${PORTA_EUREKA}/eureka/}"

mkdir -p logs
: > logs/pids

subir() {
  local modulo="$1" porta="$2" banco="${3:-}"
  local jar="${modulo}/target/${modulo}-0.0.1-SNAPSHOT.jar"
  if [ ! -f "$jar" ]; then
    echo "jar nao encontrado: $jar  (rode: mvn -DskipTests package)" >&2
    exit 1
  fi
  echo "subindo ${modulo} na porta ${porta}"
  SERVER_PORT="$porta" nohup "$JAVA" -jar "$jar" --server.port="$porta" \
      ${banco:+--spring.datasource.url="jdbc:postgresql://localhost:${DB_PORT}/${banco}"} \
      > "logs/run-${modulo}.out" 2>&1 &
  echo "$! ${modulo}" >> logs/pids
}

esperar() {
  local porta="$1" nome="$2" tentativas=90
  while [ $tentativas -gt 0 ]; do
    if curl -fsS "http://localhost:${porta}/actuator/health" >/dev/null 2>&1; then
      echo "  ${nome} pronto"
      return 0
    fi
    tentativas=$((tentativas - 1))
    sleep 2
  done
  echo "  ${nome} NAO respondeu em /actuator/health" >&2
  return 1
}

subir eureka-server "$PORTA_EUREKA"
esperar "$PORTA_EUREKA" eureka-server

subir contrato-service   "$PORTA_CONTRATO"    contrato_db
subir notificacao-service "$PORTA_NOTIFICACAO" notificacao_db
subir reputacao-service   "$PORTA_REPUTACAO"   reputacao_db
subir auditoria-service   "$PORTA_AUDITORIA"   auditoria_db
subir api-gateway         "$PORTA_GATEWAY"

esperar "$PORTA_CONTRATO" contrato-service
esperar "$PORTA_NOTIFICACAO" notificacao-service
esperar "$PORTA_REPUTACAO" reputacao-service
esperar "$PORTA_AUDITORIA" auditoria-service
esperar "$PORTA_GATEWAY" api-gateway

echo
echo "tudo no ar. gateway em http://localhost:${PORTA_GATEWAY}"
echo "para parar: scripts/parar-aplicacoes.sh"
