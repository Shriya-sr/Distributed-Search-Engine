#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/spark-common.sh"

export SPARK_LOCAL_IP="$NODE3_HOST"

echo "Starting Spark Master on Node 3..."

if jps | grep -q 'Master$'; then
    echo "Spark Master is already running."
else
    "$SPARK_HOME/sbin/start-master.sh"
fi

echo ""
echo "Spark Master URL:"
echo "spark://$NODE3_HOST:${SPARK_MASTER_PORT:-7077}"

echo ""
echo "Spark Master processes:"
jps
