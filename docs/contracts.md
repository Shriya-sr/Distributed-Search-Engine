# Integration contracts — verified against uploaded main datasets

This document distinguishes **observed source fields** from our **proposed derived columns**. The source files were inspected on 2026-11-05. Do not silently assume a raw `version` or `content` column.

## A. Source formats (confirmed)

**Phase 0:** `pages.jsonl` is newline-delimited JSON. Valid records have `doc_id: string`, `url: string`, `title: string`, `text: string`, and optionally `links: array<string>`. `links` may be absent. There is **no** `version` field. The supplied file has 1,040 nonempty lines: 1,020 valid JSON records, 20 deliberately malformed JSON lines, and 20 duplicated IDs among valid records (all duplicated copies are identical). Deduplicating valid records yields **1,000 unique documents**.

**Phase 1:** The uploaded file is named `changes.json` but its content is **JSON Lines**, not a single JSON array. It contains 100 valid change records, all with `doc_id: string` and `op: string`: 50 `DELETE` operations (only ID and op) and 50 `UPSERT` operations (ID, op, url, title, text, links). 25 upserts replace existing IDs, and 25 introduce new IDs. Applying the changes once produces **975 live unique documents** (1,000 - 50 deleted + 25 new). Reapplying must not change the result.

All `doc_id` values and `links` targets should remain **strings**; do not convert to integers or interpret IDs as URLs. Article `text` contains Wikipedia markup; any text cleaning/tokenizer must be shared between index and query handling.

## B. Ingestion behavior

- Quarantine or count malformed JSON records instead of allowing one broken line to abort a full job. Spark's permissive read mode plus corrupt-record handling is one option (implementation to be chosen and tested).
- Reject invalid records lacking the required fields for their operation. **DELETE does not require title, text, url or links.** Treat absent `links` as an empty list.
- Deduplicate by `doc_id`. The professor's brief says "highest version wins", but these provided raw files have **no version property**. For the observed identical Phase 0 duplicates, either copy has the same result; do **not** invent version numbers for source rows. If future files contain differing duplicate versions, agree on explicit precedence policy and test it.
- Phase 1 `DELETE` removes the document; `UPSERT` inserts or replaces the full document; perform link cleanup against the **final live document ID set**, not only original Phase 0 IDs.
- Phase 0's links all resolve against its 1,000 IDs; after Phase 1, 7 links target deleted/nonexistent IDs and need cleaning. After link cleanup, 467 links remain (474 before cleanup).
- Treat each phase as repeatable and idempotent. Iceberg table snapshots can change even for identical data if unnecessary writes are performed, so avoid no-op rewrites where practical.
- Use snapshot identifiers or timestamps for reproducible rollback and prove record contents before/after recovery.

## C. Proposed normalized Iceberg documents

`doc_id: string`, `url: string`, `title: string`, `text: string`, `links: array<string>`, `source_phase: integer` (derived metadata; optional, not in raw data). Name/typing must be approved by Nodes 1 and 2. If adding a normalized `version` or `content` column, document exactly how it is derived rather than claiming it exists in the raw dataset.

## D. Inverted index for BM25

- postings: `term: string`, `doc_id: string`, `tf: integer`
- document stats: `doc_id: string`, `doc_len: integer`
- corpus stats: `term: string`, `df: integer`, plus `N` and `avgdl`
- Shared tokenizer: agree on case folding, punctuation and Wikipedia markup processing. Match search-time tokenizer to indexing tokenizer exactly.

## E. PageRank

`doc_id: string`, `pagerank: double`; damping factor 0.85, at least 10 iterations, with dangling nodes accounted for. Member 4 authors; Spark driver executes on Node 3, distributed tasks on Workers 2 and 3.

## F. Versioned validated Parquet export (proposed)

```
serving/v0/
  postings/      # Parquet parts
  documents/     # Parquet parts
  pagerank/      # Parquet parts
  statistics/    # Parquet parts
  manifest.json  # completeness / validation marker
```

Node 3 publishes an export only after all writes + validation complete. Node 4 copies the complete version to local disk, validates it before activating, and keeps the last working version after failures. Node 4 never queries HDFS/Iceberg/Spark directly during search requests.

## G. Search API (proposed)

`GET /search?q=term1%20term2&limit=10` responds with `doc_id`, `title`, `url`, `score`, `version`. Score = 0.7 × normalized BM25 + 0.3 × normalized PageRank. Agree on normalization domain and zero-denominator handling. Use k1=1.2, b=0.75.

## Approval rules

Changes to source mapping, Iceberg table fields, tokenizer, export directories, ports or catalog settings require review from dependent members. Do not hard-code Wi-Fi LAN IPs; keep ZeroTier addresses in local env config, not Git.

## Course BM25 example compatibility

The professor's original example is kept at `references/bm25_reference.py`. Its documented `index_df` columns do not include `length`, although the code accesses `row["length"]`. Our contract **does not need to change**: Node 4 may join `doc_len` from document stats by `doc_id` for BM25. Document/test field naming must be aligned explicitly (`doc_len` vs `length`) before integration. See `references/README.md`.
