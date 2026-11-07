#!/usr/bin/env bash
set -euo pipefail

FILE="/distributed-search/healthcheck/node2-test.txt"

echo "=== HDFS Replication Verification ==="
echo ""

echo "Configured replication factor:"
hdfs getconf -confKey dfs.replication

echo ""
echo "Connected DataNodes:"
hdfs dfsadmin -report | grep -E \
  'Live datanodes|Name:|Under replicated blocks:'

echo ""
echo "Test file replication:"
hdfs dfs -stat 'Requested replicas: %r' "$FILE"

echo ""
echo "Block locations:"
hdfs fsck "$FILE" -files -blocks -locations

echo ""
echo "Verification complete."
echo "Full replication is confirmed only when two live"
echo "replicas appear for the test file."
