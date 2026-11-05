# Distributed Search Engine — Big Data 2026

A four-VM mini search engine for the **UE24CS343AB2** course project. This repository is a **starter scaffold**, not a completed HDFS/Spark system. Each team member can independently build and test a component before we connect our laptops.

**Official brief:** https://big-data-2026-supersite.super.site/projects-1/distributed-search-engine

## What are we building?

1. **Node 1 (Ingestion)** uploads the Wikipedia JSONL corpus, validates records, handles duplicates/updates and writes to Iceberg using a Spark ingestion driver on Node 1.
2. **Node 2 (Storage)** runs the HDFS NameNode, DataNode and one Spark Worker. Iceberg data and metadata live in HDFS.
3. **Node 3 (Compute)** runs Spark Master, Spark Worker, second HDFS DataNode, and Spark drivers for indexing, PageRank and Parquet export.
4. **Node 4 (Search)** locally loads validated Parquet exports and serves a FastAPI search UI with BM25 + precomputed PageRank. No Spark or HDFS daemons on Node 4.

**Network:** the four VMs connect through ZeroTier. **HDFS replication factor:** 2 across Nodes 2 and 3. **PageRank:** >=10 iterations, damping 0.85. **BM25:** k1=1.2 and b=0.75; normalized final score = 0.7 BM25 + 0.3 PageRank.

## Who does what?

| Person | VM | Main ownership | Code modules |
|---|---|---|---|
| Divisha | Node 2 | HDFS, Iceberg, snapshots, replication, storage tests | `storage/`, `config/hadoop/`, `config/iceberg/`, `scripts/node2/` |
| Harsimran | Node 3 | Spark cluster, jobs, Parquet export, worker-failure testing | `export/`, `config/spark/`, `scripts/node3/` |
| Member 3 (name TBD) | Node 1 | Data validation, deduplication, ingestion **and inverted-index code** | `ingestion/`, `indexing/`, `scripts/node1/` |
| Member 4 (name TBD) | Node 4 | **PageRank code**, BM25, API, UI, version fallback | `pagerank/`, `search/`, `frontend/`, `scripts/node4/` |

**Important:** *Code author* and *VM that runs the driver* are different. Inverted-index/PageRank code may be authored on another laptop, but their Spark drivers must run on Node 3. This is a proposed team allocation; the professor has not assigned algorithm authors by node. We will rebalance work if Node 4 or Node 1 becomes overloaded.

## First-time setup (all members)

Install Python 3.11+ and Git first. Spark, Hadoop, Iceberg and ZeroTier are **not installed by these commands**.

```bash
# After cloning the repository:
cd distributed-search-engine
python3 -m venv .venv
source .venv/bin/activate        # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -e '.[dev]'
python -m pytest -q
```

The initial tests check only the **starter package/contract helpers**. Passing tests does **not** mean our distributed project works.

## How we work without all laptops being connected

1. Read `docs/contracts.md` and `docs/team-workflow.md` before writing a component.
2. Build against tiny fixtures in `tests/fixtures/`; **do not** wait for the real cluster.
3. Put your code in your module folder and tests in the matching test folder.
4. Open one feature branch / pull request per issue; request a review before merge.
5. After local tests pass, the integration owners deploy the same code to the designated VM.
6. **Integration sequence:** ZeroTier → HDFS → Spark → Iceberg smoke test → ingestion → index/PageRank → export → search.

## Data and phases

The assignment supplies `pages.jsonl` (Phase 0) and a Phase 1 change file. Our uploaded Phase 1 copy is named `changes.json` **but contains JSON Lines**, not a JSON array. See `docs/dataset-audit.md` and `docs/contracts.md` for verified schemas and record counts. Do not commit the datasets to GitHub. Use the small toy dataset for development and the main corpus for the final demonstration.

Phase 0: ingest → merge into Iceberg → build index → compute PageRank → export validated Parquet → Node 4 switches to v0.

Phase 1: ingest changes **idempotently** → rebuild/update index and PageRank → export validated v1 → Node 4 switches only on successful validation; v0 remains available on failure.

## Repo layout

- `src/distributed_search/common/` — shared contracts and small helpers (changes require team approval)
- `src/distributed_search/ingestion/`, `indexing/` — Node 1 / Member 3
- `src/distributed_search/storage/` — Node 2 / Divisha
- `src/distributed_search/export/` — Node 3 / Harsimran
- `src/distributed_search/pagerank/`, `search/`, `frontend/` — Member 4
- `config/` — configuration templates, not real credentials or IP-specific secrets
- `scripts/node*/` — VM-specific setup/run scripts (to implement)
- `tests/` — independent unit tests and later integration tests
- `docs/` — contracts, setup checklist, responsibilities and demo checklist
- `planning/issues.md` — ready-to-create GitHub issues

## Project acceptance checklist

- [ ] Both HDFS DataNodes visible; replication factor = 2 verified
- [ ] Spark Master sees Workers on Nodes 2 and 3
- [ ] Ingestion rejects malformed data, deduplicates by highest version, cleans missing links
- [ ] Re-ingesting a phase does not change accepted table contents
- [ ] Iceberg snapshot history and recovery shown
- [ ] Distributed inverted index and >=10-iteration PageRank built with Spark
- [ ] BM25 + PageRank ranking uses course-specified settings
- [ ] Node 4 serves ranked results from **local validated Parquet**, not live HDFS/Iceberg
- [ ] Failed new export retains the previous searchable version
- [ ] Spark fault-tolerance demonstration completed
- [ ] Final Phase 0 and Phase 1 demonstration uses the **main corpus**

## What has NOT been implemented yet?

No production ingestion, HDFS cluster configuration, Iceberg SQL, Spark indexing/PageRank, Parquet export, API or frontend is implemented in this scaffold. Blank module files reserve ownership; issues in `planning/issues.md` track the real work.

**Next step:** source dataset fields are now audited in `docs/dataset-audit.md`; agree on implementation details of the contracts and software versions, then verify ZeroTier reachability across all VMs *before* extensive cluster setup.

## Professor-provided BM25 reference

The original provided `bm25.py` is preserved unchanged in [`references/bm25_reference.py`](references/bm25_reference.py). Please read [`references/README.md`](references/README.md) before reusing it: its documented inverted-index columns omit `length`, but the function reads `length` from the index row. Our production BM25 implementation should retrieve document lengths from document statistics or explicitly join them into the postings; the supplied scoring formula itself is fine. Node 4 also implements PageRank combination and serving.
