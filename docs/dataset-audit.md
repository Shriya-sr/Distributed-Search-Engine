# Dataset audit — uploaded main corpus (2026-11-05)

The professor's project brief: https://big-data-2026-supersite.super.site/projects-1/distributed-search-engine

**Phase 0: `pages.jsonl`** (9,834,481 bytes)
- 1,040 nonempty physical lines; first 1,020 are parseable JSON; last 20 contain deliberately broken JSON (`malformed-1` through `malformed-20`).
- 1,020 parsed rows, 1,000 distinct `doc_id` strings, 20 duplicate rows (identical duplicate copies).
- Raw keys: `doc_id`, `url`, `title`, `text`, optional `links`; 40 valid rows omit `links`.
- All valid rows have ID, URL, title, text. 499 links from valid rows; 499 from deduplicated rows (duplicates have no links); all link targets exist in Phase 0.

**Phase 1: `changes.json`** (415,297 bytes)
- 100 lines, all valid standalone JSON objects. Despite `.json` suffix, parse as JSONL.
- 50 DELETE: `doc_id`, `op` only. 50 UPSERT: `doc_id`, `op`, `url`, `title`, `text`, `links`.
- DELETE IDs are all present in Phase 0; UPSERT replaces 25 existing IDs and inserts 25 new IDs.
- Expected post-change documents: 975 distinct live IDs. After applying changes, there are 474 raw outgoing links, of which 7 point to absent/deleted IDs; 467 remain after removing missing-target links.

**Important considerations**
- No raw `version` field exists in either file. The brief's highest-version language therefore does not directly apply to these provided duplicate rows; do not fabricate versions without a documented policy.
- Raw text is Wikipedia wikitext, not plain text. Query and indexing tokenize consistently.
- `doc_id` and link IDs are strings; avoid integer coercion.
- Expected counts above are audit checks, not substitutes for distributed Spark/HDFS correctness tests.
- Do not commit the full corpus to Git; it should stay in each member's ignored `data/` folder.
