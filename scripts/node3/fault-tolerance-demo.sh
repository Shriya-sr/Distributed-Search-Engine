#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/spark-job-common.sh"

WAIT_SECONDS="${WAIT_SECONDS:-60}"

check_master() {
    python3 - "$SPARK_MASTER_WEBUI_URL/json/" <<'PY'
import json
import sys
import urllib.request

try:
    with urllib.request.urlopen(sys.argv[1], timeout=4) as response:
        data = json.load(response)
    if not data.get("url", "").startswith("spark://"):
        raise SystemExit(1)
except Exception:
    raise SystemExit(1)
PY
}

worker_has_state() {
    local expected="$1"

    python3 - "$SPARK_MASTER_WEBUI_URL/json/" "$NODE3_HOST" "$expected" <<'PY'
import json
import sys
import urllib.request

url, node_host, expected = sys.argv[1:]
try:
    with urllib.request.urlopen(url, timeout=4) as response:
        data = json.load(response)
    workers = data.get("workers", [])
    alive = any(
        worker.get("host", "").split(":")[0] == node_host
        and worker.get("state") == "ALIVE"
        for worker in workers
    )
    raise SystemExit(0 if alive == (expected == "alive") else 1)
except Exception:
    raise SystemExit(1)
PY
}

wait_for_worker_state() {
    local expected="$1"
    local description="$2"

    for ((second=0; second<WAIT_SECONDS; second++)); do
        if ! check_master; then
            echo "ERROR: Spark Master is not responding at $SPARK_MASTER_WEBUI_URL." >&2
            return 1
        fi

        if worker_has_state "$expected"; then
            echo "$description"
            return 0
        fi
        sleep 1
    done

    echo "ERROR: Timed out waiting for Node 3 Worker to become $expected." >&2
    return 1
}

echo "Checking Spark Master..."
if ! check_master; then
    echo "ERROR: Spark Master is not responding. Start the Master before this demo." >&2
    exit 1
fi

if ! worker_has_state alive; then
    echo "ERROR: Node 3 Worker is not currently registered as ALIVE." >&2
    exit 1
fi

echo "Node 3 Worker is ALIVE. Stopping only the local Worker..."
"$SPARK_HOME/sbin/stop-worker.sh"

wait_for_worker_state absent \
    "Confirmed: Node 3 Worker stopped; Spark Master is still responding."

echo "Restarting Node 3 Worker..."
"$SPARK_HOME/sbin/start-worker.sh" "$SPARK_MASTER_URL"

wait_for_worker_state alive \
    "SUCCESS: Node 3 Worker has re-registered; Spark Master remained available."

echo
echo "Fault-tolerance check complete."
echo "This verifies local Worker stop/restart and Master availability."
echo "A distributed job surviving a worker failure requires Node 2's Worker"
echo "to be registered and a suitable distributed job to be running."
