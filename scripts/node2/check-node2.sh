#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "=== Node 2 Service Health Check ==="

echo ""
echo "1. Hadoop NameNode and DataNode"
jps

echo ""
echo "2. HDFS configuration"
hdfs getconf -confKey fs.defaultFS
hdfs getconf -confKey dfs.replication

echo ""
echo "3. HDFS safe mode"
hdfs dfsadmin -safemode get

echo ""
echo "4. HDFS DataNodes"
hdfs dfsadmin -report | grep -E \
    'Live datanodes|Name:|Under replicated blocks:'

echo ""
echo "5. Spark Worker status"
if jps | grep -q ' Worker$'; then
    echo "Spark Worker is running."
else
    echo "Spark Worker is not running yet."
    echo "This is expected before cross-node integration."
fi

echo ""
echo "6. Iceberg snapshot inspection"
"$PROJECT_ROOT/scripts/node2/inspect-snapshots.sh" search.db.pages
