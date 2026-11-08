# Node 2 Operations Runbook

## Responsibilities

Node 2 runs:
- HDFS NameNode and DataNode
- HDFS-backed Apache Iceberg catalog
- Spark Worker

## Local environment

- Hadoop 3.3.6: /opt/hadoop
- Spark 3.3.4: /opt/spark
- Iceberg 1.8.1
- Java 11
- Node 2 ZeroTier IP: 172.25.124.60
- Node 3 ZeroTier IP: 172.25.240.223

Create a local .env file using config/cluster.example.env.
Set SPARK_HOME, JAVA_HOME, NODE2_HOST and NODE3_HOST.
Never commit .env.

## HDFS operations

Start:
    ./scripts/node2/start-hdfs.sh

Check:
    ./scripts/node2/check-hdfs.sh

Verify replication:
    ./scripts/node2/verify-replication.sh

Stop:
    ./scripts/node2/stop-hdfs.sh

Do not format the NameNode or delete Hadoop storage.

## Iceberg operations

Create tables:
    Run config/iceberg/create-tables.sql using Spark SQL
    with the Iceberg Spark runtime JAR and HDFS catalog settings.

Inspect snapshots:
    ./scripts/node2/inspect-snapshots.sh search.db.pages

Read a previous version:
    ./scripts/node2/recover-snapshot.sh search.db.pages SNAPSHOT_ID view

Restore a previous version:
    ./scripts/node2/recover-snapshot.sh search.db.pages SNAPSHOT_ID rollback

Rollback requires explicit confirmation.
Inspect the target snapshot before restoring.

## Spark Worker

Node 3 hosts the Spark Master.

When integration begins, start the Node 2 worker using:
    ./scripts/node2/start-spark-worker.sh

The Worker uses scripts/node3/start-spark-worker.sh.
The configured master is spark://172.25.240.223:7077.

## Health check

    ./scripts/node2/check-node2.sh

## Automated tests

Activate the local virtual environment, then run:
    python -m pytest -q tests/test_node2_operations.py

## Known integration dependencies

HDFS replication factor is 2.
Until Node 3 joins, blocks may be under-replicated.

The Spark Worker has not yet been connected to the
Node 3 Spark Master.

Cross-VM networking, replication and Spark registration
will be verified during integration testing.
