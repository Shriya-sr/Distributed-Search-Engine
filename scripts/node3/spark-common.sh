#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

ENV_FILE="${CLUSTER_ENV_FILE:-$PROJECT_ROOT/.env}"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: Missing $ENV_FILE"
    echo "Copy config/cluster.example.env to .env and configure it."
    exit 1
fi

set -a
source "$ENV_FILE"
set +a

: "${SPARK_HOME:?Set SPARK_HOME in .env}"
: "${NODE3_HOST:?Set NODE3_HOST in .env}"

export SPARK_CONF_DIR="$PROJECT_ROOT/config/spark"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

if [[ -n "${JAVA_HOME:-}" ]]; then
    export JAVA_HOME
fi

if [[ ! -x "$SPARK_HOME/sbin/start-master.sh" ]]; then
    echo "ERROR: Spark start-master.sh not found under SPARK_HOME"
    exit 1
fi

if [[ ! -x "$SPARK_HOME/sbin/start-worker.sh" &&
      ! -x "$SPARK_HOME/sbin/start-slave.sh" ]]; then
    echo "ERROR: Spark worker startup script not found"
    exit 1
fi

spark_worker_start() {
    local master_url="$1"

    if [[ -x "$SPARK_HOME/sbin/start-worker.sh" ]]; then
        "$SPARK_HOME/sbin/start-worker.sh" "$master_url"
    else
        "$SPARK_HOME/sbin/start-slave.sh" "$master_url"
    fi
}

spark_worker_stop() {
    if [[ -x "$SPARK_HOME/sbin/stop-worker.sh" ]]; then
        "$SPARK_HOME/sbin/stop-worker.sh"
    elif [[ -x "$SPARK_HOME/sbin/stop-slave.sh" ]]; then
        "$SPARK_HOME/sbin/stop-slave.sh"
    fi
}
