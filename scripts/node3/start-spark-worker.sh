#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/spark-common.sh"

ROLE="${1:-}"

case "$ROLE" in
    node2)
        : "${NODE2_HOST:?Set NODE2_HOST in .env}"
        export SPARK_LOCAL_IP="$NODE2_HOST"
        export SPARK_WORKER_WEBUI_PORT="${NODE2_WORKER_WEBUI_PORT:-8081}"
        NODE_NAME="Node 2"
        ;;
    node3)
        export SPARK_LOCAL_IP="$NODE3_HOST"
        export SPARK_WORKER_WEBUI_PORT="${NODE3_WORKER_WEBUI_PORT:-8082}"
        NODE_NAME="Node 3"
        ;;
    *)
        echo "Usage: $0 node2|node3"
        exit 1
        ;;
esac

MASTER_URL="spark://${NODE3_HOST}:${SPARK_MASTER_PORT:-7077}"

echo "Starting Spark Worker on $NODE_NAME..."
echo "Connecting to $MASTER_URL"

if jps | grep -q 'Worker$'; then
    echo "A Spark Worker is already running."
else
    spark_worker_start "$MASTER_URL"
fi

echo ""
echo "Running Java processes:"
jps
