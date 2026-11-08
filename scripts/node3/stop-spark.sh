#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/spark-common.sh"

echo "Stopping Spark Worker..."

spark_worker_stop || true

echo ""
echo "Stopping Spark Master if running..."

if [[ -x "$SPARK_HOME/sbin/stop-master.sh" ]]; then
    "$SPARK_HOME/sbin/stop-master.sh"
fi

echo ""
echo "Remaining Java processes:"
jps
