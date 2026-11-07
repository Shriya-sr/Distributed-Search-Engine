#!/usr/bin/env bash
set -euo pipefail

echo "=== Node 2 HDFS Health Check ==="

echo ""
echo "1. Checking Hadoop daemons"
jps

echo ""
echo "2. Checking NameNode address"
hdfs getconf -confKey fs.defaultFS

echo ""
echo "3. Checking replication configuration"
hdfs getconf -confKey dfs.replication

echo ""
echo "4. Checking HDFS safe mode"
hdfs dfsadmin -safemode get

echo ""
echo "5. Checking DataNode status"
hdfs dfsadmin -report

echo ""
echo "6. Checking project HDFS directory"
hdfs dfs -ls /distributed-search

echo ""
echo "7. Checking project filesystem health"
hdfs fsck /distributed-search
