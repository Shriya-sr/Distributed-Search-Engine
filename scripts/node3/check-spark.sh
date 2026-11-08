#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/spark-common.sh"

MASTER_URL="http://${NODE3_HOST}:${SPARK_MASTER_WEBUI_PORT:-8080}"

echo "=== Spark Cluster Health Check ==="

echo ""
echo "1. Local Java processes"
jps

echo ""
echo "2. Spark Master Web UI"

if ! command -v curl >/dev/null 2>&1; then
    echo "ERROR: curl is required."
    exit 1
fi

if ! curl --fail --silent --show-error \
    "$MASTER_URL/json/" > /tmp/spark-master-status.json; then
    echo "ERROR: Spark Master is unreachable."
    exit 1
fi

echo "Spark Master is reachable."

echo ""
echo "3. Registered Workers"

python - <<'PY'
import json

with open("/tmp/spark-master-status.json", encoding="utf-8") as file:
    status = json.load(file)

workers = status.get("workers", [])

print(f"Registered workers: {len(workers)}")

for worker in workers:
    print(
        f"- {worker.get('host')} "
        f"| state={worker.get('state')} "
        f"| cores={worker.get('cores')}"
    )

alive = [
    worker for worker in workers
    if worker.get("state") == "ALIVE"
]

if len(alive) < 2:
    raise SystemExit(
        "ERROR: Expected two ALIVE Spark Workers."
    )

print("SUCCESS: Both Spark Workers are registered.")
PY

echo ""
echo "=== Spark Health Check Complete ==="
