#!/usr/bin/env bash
set -euo pipefail

echo "Starting Node 2 HDFS services..."

if ! jps | grep -q ' NameNode$'; then
    hdfs --daemon start namenode
else
    echo "NameNode is already running."
fi

if ! jps | grep -q ' DataNode$'; then
    hdfs --daemon start datanode
else
    echo "DataNode is already running."
fi

echo ""
echo "Running Hadoop processes:"
jps

echo ""
echo "HDFS NameNode: $(hdfs getconf -confKey fs.defaultFS)"
echo "Replication factor: $(hdfs getconf -confKey dfs.replication)"
