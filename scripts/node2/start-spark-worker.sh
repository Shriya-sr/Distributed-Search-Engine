#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SHARED_SCRIPT="$PROJECT_ROOT/scripts/node3/start-spark-worker.sh"

if [[ ! -x "$SHARED_SCRIPT" ]]; then
    echo "ERROR: Shared Spark Worker launcher not found."
    echo "Merge the Spark cluster PR before running this script."
    exit 1
fi

exec "$SHARED_SCRIPT" node2
