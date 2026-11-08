#!/usr/bin/env bash
set -euo pipefail

TABLE="${1:-}"
SNAPSHOT="${2:-}"
ACTION="${3:-}"

if [[ ! "$TABLE" =~ ^search\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
    echo "Usage: $0 search.db.table SNAPSHOT_ID view|rollback"
    exit 1
fi

if [[ ! "$SNAPSHOT" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Snapshot ID must be numeric."
    exit 1
fi

if [[ "$ACTION" != "view" && "$ACTION" != "rollback" ]]; then
    echo "ERROR: Action must be view or rollback."
    exit 1
fi

SPARK_ARGS=(
    --master 'local[2]'
    --jars /opt/iceberg/iceberg-spark-runtime-3.3_2.12-1.8.1.jar
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    --conf spark.sql.catalog.search=org.apache.iceberg.spark.SparkCatalog
    --conf spark.sql.catalog.search.type=hadoop
    --conf spark.sql.catalog.search.warehouse=hdfs://node2:9000/distributed-search/warehouse
)

if [[ "$ACTION" == "view" ]]; then
    echo "Viewing $TABLE at snapshot $SNAPSHOT"
    spark-sql "${SPARK_ARGS[@]}" \
        -e "SELECT * FROM $TABLE VERSION AS OF $SNAPSHOT"
    exit 0
fi

echo "WARNING: This will change the current state of $TABLE."
echo "Target snapshot: $SNAPSHOT"
read -r -p "Type ROLLBACK to confirm: " CONFIRM

if [[ "$CONFIRM" != "ROLLBACK" ]]; then
    echo "Cancelled."
    exit 1
fi

spark-sql "${SPARK_ARGS[@]}" \
    -e "CALL search.system.rollback_to_snapshot(
        '${TABLE#search.}',
        $SNAPSHOT
    )"

echo "Rollback command completed."
