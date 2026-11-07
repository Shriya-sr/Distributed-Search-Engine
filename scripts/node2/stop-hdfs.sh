#!/usr/bin/env bash
set -euo pipefail

echo "Stopping Node 2 HDFS services..."

if jps | grep -q ' DataNode$'; then
    hdfs --daemon stop datanode
else
    echo "DataNode is not running."
fi

if jps | grep -q ' NameNode$'; then
    hdfs --daemon stop namenode
else
    echo "NameNode is not running."
fi

echo ""
echo "Remaining Java processes:"
jps
