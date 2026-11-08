#!/usr/bin/env bash

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$PROJECT_ROOT/.env"
    set +a
fi

: "${SPARK_HOME:=/opt/spark}"
: "${JAVA_HOME:=/usr/lib/jvm/java-11-openjdk-amd64}"
: "${SPARK_MASTER_PORT:=7077}"
: "${SPARK_MASTER_WEBUI_PORT:=8080}"
: "${SPARK_WORKER_PORT:=7078}"
: "${SPARK_WORKER_WEBUI_PORT:=8082}"
: "${SPARK_WORKER_CORES:=2}"
: "${SPARK_WORKER_MEMORY:=2g}"

if [[ -z "${NODE3_HOST:-}" ]]; then
    echo "ERROR: NODE3_HOST is not set in .env." >&2
    exit 1
fi

export SPARK_HOME JAVA_HOME
export SPARK_LOCAL_IP="$NODE3_HOST"
export SPARK_WORKER_PORT SPARK_WORKER_WEBUI_PORT
export SPARK_WORKER_CORES SPARK_WORKER_MEMORY
export PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://${NODE3_HOST}:${SPARK_MASTER_PORT}}"
SPARK_MASTER_WEBUI_URL="${SPARK_MASTER_WEBUI_URL:-http://${NODE3_HOST}:${SPARK_MASTER_WEBUI_PORT}}"
export SPARK_MASTER_URL SPARK_MASTER_WEBUI_URL

if [[ ! -x "$SPARK_HOME/sbin/start-worker.sh" || ! -x "$SPARK_HOME/sbin/stop-worker.sh" ]]; then
    echo "ERROR: Spark worker start/stop scripts were not found under $SPARK_HOME." >&2
    exit 1
fi
