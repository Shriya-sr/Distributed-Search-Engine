# Draft GitHub issues — not yet created remotely

## [Shared] Confirm audited source schemas and pin software versions

**Owner:** Shared

Review docs/dataset-audit.md and docs/contracts.md. Confirm JSONL parsing despite changes.json suffix, missing version field and malformed Phase 0 records; then pin Java, Spark, Hadoop, Iceberg, Python and VM runtime versions.

**Done when:** contracts approved and tested software version matrix documented.

## [Shared] Approve contracts and toy test fixtures

**Owner:** Shared

Agree on document keys, links, update/deletes, tokenization, postings, Parquet serving layout and API.

**Done when:** docs/contracts.md approved and fixture files agreed.

## [Node 2] Set up HDFS NameNode/DataNode with replication

**Owner:** Node 2 / Divisha

Configure Node 2 and coordinate Node 3 DataNode. Verify actual replication=2 and file read.

**Done when:** Two DataNodes healthy; fsck / replication evidence recorded.

## [Node 2] Set up Iceberg catalog, tables and snapshots

**Owner:** Node 2 / Divisha

Configure Iceberg in HDFS and implement snapshot history and rollback procedures.

**Done when:** Spark writes and reads Iceberg; snapshots verified; rollback demonstrated.

## [Node 2] Add storage startup, health-check and recovery scripts

**Owner:** Node 2 / Divisha

Document startup order, logs, storage health, restart procedure.

**Done when:** Scripts and reproducible storage runbook committed.

## [Node 3] Set up Spark Master and two Workers

**Owner:** Node 3 / Harsimran

Configure master on Node 3 and workers on Nodes 2 and 3; run distributed sample job.

**Done when:** Workers register and run distributed job.

## [Node 3] Implement validated Parquet export

**Owner:** Node 3 / Harsimran

Publish versioned index, documents, stats and PageRank with validation manifest.

**Done when:** Failed export cannot mark version complete; Node 4 can read complete export.

## [Node 3] Create job runner and fault-tolerance demonstration

**Owner:** Node 3 / Harsimran

Run indexing/PageRank drivers on Node 3; simulate worker failure, log recovery.

**Done when:** Reproducible job launch and Spark failure test documented.

## [Node 1] Implement Phase 0/1 input validation and deduplication

**Owner:** Node 1 / Member 3

Parse pages.jsonl (including 20 malformed lines), changes.json (JSONL; 50 DELETE + 50 UPSERT), handle missing links, deduplicate 20 identical duplicate IDs, apply operations, clean dangling links after deletes. No raw version column exists.

**Done when:** tests verify counts 1000 unique Phase 0 IDs and 975 unique Phase 1 IDs, invalid rows rejected and idempotency.

## [Node 1] Implement idempotent Iceberg ingestion

**Owner:** Node 1 / Member 3

Spark driver on Node 1 submits to Node 3 master; merge changes into Iceberg.

**Done when:** Re-ingesting the same phase leaves table contents unchanged.

## [Index] Implement distributed inverted index

**Owner:** Node 1 / Member 3

Build postings term/doc/tf and BM25 stats with Spark; driver executed on Node 3.

**Done when:** Toy expected postings and statistics match, distributed execution verified.

## [PageRank] Implement iterative Spark PageRank

**Owner:** Node 4 / Member 4

Implement damping .85, >=10 iterations, dangling handling, driver run on Node 3.

**Done when:** Toy graph expected scores, normalization and iteration count checked.

## [Search] Implement BM25 and score combination

**Owner:** Node 4 / Member 4

Use k1=1.2 b=.75 and 0.7 normalized BM25 + 0.3 normalized PageRank.

**Done when:** Deterministic ranking unit tests pass.

## [Search] Build FastAPI and basic frontend

**Owner:** Node 4 / Member 4

Load completed local Parquet export and return ranked docs with identifying fields.

**Done when:** Search works without live Spark/HDFS daemons on Node 4.

## [Search] Implement safe version activation and fallback

**Owner:** Node 4 / Member 4

Activate only validated completed exports; keep v0 on failed v1.

**Done when:** Failure test proves old search version is still served.

## [Integration] Complete Phase 0/1 main-corpus demo

**Owner:** Everyone

Run Phase 0 then apply changes.jsonl; verify search version, Iceberg snapshots, fault tolerance.

**Done when:** Main corpus demonstrated with logs, timings and viva explanations.

