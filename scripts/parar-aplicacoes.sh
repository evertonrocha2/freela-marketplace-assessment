#!/usr/bin/env bash
# Encerra as aplicacoes iniciadas por subir-aplicacoes.sh.
set -uo pipefail
cd "$(dirname "$0")/.."

if [ ! -f logs/pids ]; then
  echo "logs/pids nao existe; nada para parar"
  exit 0
fi

while read -r pid nome; do
  [ -z "${pid:-}" ] && continue
  if kill "$pid" 2>/dev/null; then
    echo "parado ${nome} (pid ${pid})"
  else
    echo "ja estava parado ${nome} (pid ${pid})"
  fi
done < logs/pids

rm -f logs/pids
