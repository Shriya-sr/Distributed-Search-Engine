#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/spark-job-common.sh"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 [runner options] <spark-application.py|.jar> [application arguments...]" >&2
    exit 2
fi

cd "$PROJECT_ROOT"
exec python -m distributed_search.jobs.runner \
    --master "$SPARK_MASTER_URL" \
    "$@"
