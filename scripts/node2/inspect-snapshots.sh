#!/usr/bin/env bash
set -euo pipefail

TABLE="${1:-search.db.pages}"

if [[ ! "$TABLE" =~ ^search\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
    echo "ERROR: Expected table name like search.db.pages"
    exit 1
fi

echo "=== Iceberg Snapshot Inspection ==="
echo "Table: $TABLE"

spark-sql \
  --master 'local[2]' \
  --jars /opt/iceberg/iceberg-spark-runtime-3.3_2.12-1.8.1.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.search=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.search.type=hadoop \
  --conf spark.sql.catalog.search.warehouse=hdfs://node2:9000/distributed-search/warehouse \
  -e "SELECT snapshot_id, parent_id, operation, committed_at
      FROM ${TABLE}.snapshots
      ORDER BY committed_at"
