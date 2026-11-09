#!/usr/bin/env python3
"""
scripts/node1/run_ingestion.py
Node 1 ingestion runner — Phase 0 and Phase 1 with PySpark + Iceberg.

─────────────────────────────────────────────────────────────────────────────
Usage (on VM1, venv activated):

  # Phase 0 — parse + validate + dedup pages.jsonl, write to Iceberg
  python scripts/node1/run_ingestion.py --phase 0

  # Phase 1 — read existing Iceberg table, apply changes, write back
  python scripts/node1/run_ingestion.py --phase 1

  # Dry-run (validation only, no Spark / Iceberg write)
  python scripts/node1/run_ingestion.py --phase 0 --dry-run
  python scripts/node1/run_ingestion.py --phase 1 --dry-run

─────────────────────────────────────────────────────────────────────────────
Required environment variables (put in .env at repo root, never commit it):

  SPARK_MASTER_URL    e.g.  spark://10.147.17.x:7077   (Node 3 ZeroTier IP)
  ICEBERG_WAREHOUSE   e.g.  hdfs://node2:9000/distributed-search/warehouse

Optional environment variables:

  ICEBERG_JAR_PACKAGE Maven coordinates for the Iceberg Spark runtime jar.
                      Defaults to:
                      org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.8.1
  PAGES_PATH          Override path to Phase 0 JSONL  (default: data/pages.jsonl)
  CHANGES_PATH        Override path to Phase 1 JSONL  (default: data/changes.json)

─────────────────────────────────────────────────────────────────────────────
Idempotency design:

  Phase 0:  MERGE INTO search.db.pages by doc_id.
            Re-running with the same file leaves the table unchanged.

  Phase 1:  (1) Read the CURRENT Iceberg table into memory.
            (2) Apply DELETE + UPSERT operations using the tested Python logic.
            (3) Clean dangling links against the final live ID set.
            (4a) DELETE rows whose doc_id is no longer in the live set.
            (4b) MERGE remaining + new docs by doc_id (UPDATE or INSERT).
            Re-running with the same changes.json leaves the table unchanged.

─────────────────────────────────────────────────────────────────────────────
Iceberg table (defined by VM2 in config/iceberg/create-tables.sql):

  search.db.pages (
      doc_id  STRING,
      url     STRING,
      title   STRING,
      text    STRING,
      links   ARRAY<STRING>
  ) USING iceberg TBLPROPERTIES ('format-version' = '2')
─────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Make the src package importable when this script is run from the repo root.
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from distributed_search.ingestion import (
    apply_phase1,
    clean_dangling_links,
    deduplicate_pages,
    load_changes_jsonl,
    load_pages_jsonl,
    validate_page,
)

# ---------------------------------------------------------------------------
# Constants / defaults
# ---------------------------------------------------------------------------
_DEFAULT_PAGES_PATH = "data/pages.jsonl"
_DEFAULT_CHANGES_PATH = "data/changes.json"
_ICEBERG_TABLE = "search.db.pages"
_DEFAULT_JAR = (
    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.8.1"
)
_DEFAULT_WAREHOUSE = "hdfs://node2:9000/distributed-search/warehouse"


# ---------------------------------------------------------------------------
# Spark schema — must exactly match config/iceberg/create-tables.sql
# ---------------------------------------------------------------------------
def _doc_schema():
    """Return the PySpark StructType for the pages table.

    Import is deferred so that the module loads without PySpark installed
    (dry-run mode, CI, unit tests).
    """
    from pyspark.sql.types import (  # type: ignore[import]
        ArrayType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("doc_id", StringType(), nullable=False),
            StructField("url", StringType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("text", StringType(), nullable=False),
            StructField("links", ArrayType(StringType()), nullable=False),
        ]
    )


# ---------------------------------------------------------------------------
# SparkSession factory
# ---------------------------------------------------------------------------
def _get_spark(app_name: str = "node1-ingestion"):
    """Create (or reuse) a SparkSession configured for Iceberg.

    Reads SPARK_MASTER_URL and ICEBERG_WAREHOUSE from the environment.
    Raises RuntimeError if either variable is missing.
    """
    from pyspark.sql import SparkSession  # type: ignore[import]

    master = os.environ.get("SPARK_MASTER_URL", "").strip()
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", _DEFAULT_WAREHOUSE).strip()
    if not warehouse:
        warehouse = _DEFAULT_WAREHOUSE
    jar_pkg = os.environ.get("ICEBERG_JAR_PACKAGE", _DEFAULT_JAR).strip()

    if not master:
        raise RuntimeError(
            "SPARK_MASTER_URL is not set. "
            "Export it from your .env file before running."
        )

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # ── Iceberg catalog wired to 'search' (matches search.db.pages) ──
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.search", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.search.type", "hadoop")
        .config("spark.sql.catalog.search.warehouse", warehouse)
        # ── Pull the Iceberg runtime jar automatically ──────────────────
        .config("spark.jars.packages", jar_pkg)
        .getOrCreate()
    )

    # Silence INFO/WARN noise so our print() output is easy to read
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Pure-Python ingestion helpers (no Spark dependency)
# ---------------------------------------------------------------------------
def _load_and_validate_pages(pages_path: str) -> dict[str, dict]:
    """Load pages.jsonl → validate → deduplicate → return live_docs dict."""
    print(f"[Phase 0] Loading pages from {pages_path} …")
    records, quarantined = load_pages_jsonl(pages_path)
    print(f"  Parsed {len(records)} records, quarantined {quarantined} malformed lines.")

    valid: list[dict] = []
    invalid_count = 0
    for rec in records:
        ok, reason = validate_page(rec)
        if ok:
            valid.append(rec)
        else:
            invalid_count += 1

    print(f"  Valid: {len(valid)}, Invalid (field-level rejection): {invalid_count}")

    live_docs = deduplicate_pages(valid)
    print(f"  After dedup: {len(live_docs)} unique documents.")
    return live_docs


def _load_and_apply_changes(
    live_docs: dict[str, dict], changes_path: str
) -> dict[str, dict]:
    """Load changes.json → apply operations → clean links → return updated live_docs."""
    print(f"[Phase 1] Loading changes from {changes_path} …")
    records, quarantined = load_changes_jsonl(changes_path)
    print(
        f"  Parsed {len(records)} change records, quarantined {quarantined} malformed lines."
    )

    new_live, rejected = apply_phase1(live_docs, records)
    if rejected:
        print(f"  Rejected {len(rejected)} invalid change records.")
    print(f"  After apply: {len(new_live)} live documents.")

    cleaned = clean_dangling_links(new_live)
    total_links = sum(len(d.get("links", [])) for d in cleaned.values())
    print(f"  After link cleanup: {total_links} total outgoing links survive.")
    return cleaned


# ---------------------------------------------------------------------------
# Iceberg read helper
# ---------------------------------------------------------------------------
def _read_iceberg(spark) -> dict[str, dict]:
    """Read the entire search.db.pages Iceberg table into a Python dict.

    Returns:
        dict of doc_id -> document dict, ready for the Python ingestion
        functions (apply_phase1, clean_dangling_links, etc.).
    Raises:
        RuntimeError if the table does not exist (Phase 0 must run first).
    """
    try:
        df = spark.sql(f"SELECT doc_id, url, title, text, links FROM {_ICEBERG_TABLE}")
    except Exception as exc:
        raise RuntimeError(
            f"Could not read {_ICEBERG_TABLE}. "
            "Make sure Phase 0 has been ingested and the Iceberg table exists. "
            f"Original error: {exc}"
        ) from exc

    rows = df.collect()
    print(f"  Read {len(rows)} existing documents from {_ICEBERG_TABLE}.")

    return {
        row["doc_id"]: {
            "doc_id": row["doc_id"],
            "url": row["url"],
            "title": row["title"],
            "text": row["text"],
            "links": list(row["links"]) if row["links"] else [],
        }
        for row in rows
    }


# ---------------------------------------------------------------------------
# Iceberg write helpers
# ---------------------------------------------------------------------------
def _docs_to_dataframe(live_docs: dict[str, dict], spark):
    """Convert a live_docs Python dict into a typed Spark DataFrame."""
    rows = [
        (
            doc["doc_id"],
            doc["url"],
            doc["title"],
            doc["text"],
            doc.get("links", []),
        )
        for doc in live_docs.values()
    ]
    return spark.createDataFrame(rows, schema=_doc_schema())


def _merge_into_iceberg(live_docs: dict[str, dict], spark) -> None:
    """Idempotently MERGE live_docs into search.db.pages by doc_id.

    Rows already in the table are updated; new rows are inserted.
    Rows absent from live_docs are LEFT UNCHANGED by this call — use
    _delete_from_iceberg() beforehand to remove them.
    """
    df = _docs_to_dataframe(live_docs, spark)
    df.createOrReplaceTempView("_node1_staging")

    spark.sql(f"""
        MERGE INTO {_ICEBERG_TABLE} AS t
        USING _node1_staging AS s
            ON t.doc_id = s.doc_id
        WHEN MATCHED THEN
            UPDATE SET
                t.url   = s.url,
                t.title = s.title,
                t.text  = s.text,
                t.links = s.links
        WHEN NOT MATCHED THEN
            INSERT (doc_id, url, title, text, links)
            VALUES (s.doc_id, s.url, s.title, s.text, s.links)
    """)


def _delete_from_iceberg(doc_ids_to_delete: set[str], spark) -> None:
    """Delete the given doc_ids from search.db.pages.

    Safe to call with an empty set (no-op). Idempotent: deleting an already-
    absent ID has no effect.
    """
    if not doc_ids_to_delete:
        return

    # Build a quoted, comma-separated list for the IN predicate.
    # doc_ids are plain integer strings from the dataset so quoting is safe.
    id_list = ", ".join(f"'{did}'" for did in sorted(doc_ids_to_delete))
    spark.sql(f"DELETE FROM {_ICEBERG_TABLE} WHERE doc_id IN ({id_list})")
    print(f"  Deleted {len(doc_ids_to_delete)} documents from Iceberg.")


# ---------------------------------------------------------------------------
# Top-level phase runners
# ---------------------------------------------------------------------------
def run_phase0(pages_path: str, spark=None) -> None:
    """
    Phase 0 ingestion:
      1. Load + validate + dedup pages.jsonl  (pure Python)
      2. MERGE INTO search.db.pages            (Spark / Iceberg)

    When spark is None the write step is skipped (dry-run mode).
    """
    live_docs = _load_and_validate_pages(pages_path)

    if spark is None:
        print("[dry-run] Skipping Iceberg write.")
        return

    print(f"  Merging {len(live_docs)} documents into {_ICEBERG_TABLE} …")
    _merge_into_iceberg(live_docs, spark)
    print(f"  Phase 0 complete. Iceberg snapshot updated.")


def run_phase1(changes_path: str, spark=None, pages_path: str | None = None) -> None:
    """
    Phase 1 ingestion:
      1a. If spark is available: read the CURRENT Iceberg table as live_docs.
      1b. If dry-run: re-load Phase 0 from disk (for validation only).
      2.  Apply changes (DELETE / UPSERT) using tested Python logic.
      3.  Clean dangling links against the final live ID set.
      4.  Write back to Iceberg:
            (a) DELETE rows whose doc_id is no longer in the live set.
            (b) MERGE survivors + new docs (UPDATE or INSERT).

    Both step (a) and (b) are idempotent: re-running with the same
    changes.json leaves search.db.pages unchanged.
    """
    if spark is not None:
        # Production path: read the already-ingested Phase 0 state from Iceberg.
        print("[Phase 1] Reading current state from Iceberg …")
        before_live_docs = _read_iceberg(spark)
    else:
        # Dry-run path: reconstruct Phase 0 state from disk for validation.
        if pages_path is None:
            pages_path = _DEFAULT_PAGES_PATH
        print("[Phase 1 / dry-run] Reconstructing Phase 0 from disk …")
        before_live_docs = _load_and_validate_pages(pages_path)

    before_ids = set(before_live_docs.keys())

    after_live_docs = _load_and_apply_changes(before_live_docs, changes_path)
    after_ids = set(after_live_docs.keys())

    if spark is None:
        print("[dry-run] Skipping Iceberg write.")
        return

    # ── Write back ──────────────────────────────────────────────────────────
    ids_to_delete = before_ids - after_ids   # deleted by this phase
    ids_to_upsert = after_ids                # survivors updated + new inserts

    print(
        f"  Writing Phase 1 to Iceberg: "
        f"{len(ids_to_delete)} deletes, {len(ids_to_upsert)} upserts …"
    )
    _delete_from_iceberg(ids_to_delete, spark)
    _merge_into_iceberg(after_live_docs, spark)

    print(
        f"  Phase 1 complete. {len(after_live_docs)} live documents in Iceberg."
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Node 1 ingestion runner (PySpark + Iceberg)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment variables required for live writes:
  SPARK_MASTER_URL   e.g. spark://10.147.17.x:7077
  ICEBERG_WAREHOUSE  e.g. hdfs://node2:9000/distributed-search/warehouse (default: hdfs://node2:9000/distributed-search/warehouse)

Load them before running:
  source .env   # or export each variable manually
        """,
    )
    parser.add_argument(
        "--phase",
        type=int,
        choices=[0, 1],
        required=True,
        help="Ingestion phase: 0 for pages.jsonl, 1 for changes.json",
    )
    parser.add_argument(
        "--pages",
        default=os.environ.get("PAGES_PATH", _DEFAULT_PAGES_PATH),
        help="Path to Phase 0 pages JSONL file",
    )
    parser.add_argument(
        "--changes",
        default=os.environ.get("CHANGES_PATH", _DEFAULT_CHANGES_PATH),
        help="Path to Phase 1 changes JSONL file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print counts only; do not create a Spark session or write to Iceberg.",
    )
    args = parser.parse_args()

    spark = None
    if not args.dry_run:
        print("Starting Spark session …")
        spark = _get_spark()
        print(f"  Connected to master: {os.environ.get('SPARK_MASTER_URL')}")
        print(f"  Iceberg warehouse:   {os.environ.get('ICEBERG_WAREHOUSE', _DEFAULT_WAREHOUSE)}")

    if args.phase == 0:
        run_phase0(pages_path=args.pages, spark=spark)
    else:
        run_phase1(
            changes_path=args.changes,
            spark=spark,
            pages_path=args.pages,
        )

    if spark is not None:
        spark.stop()

    print("Done.")


if __name__ == "__main__":
    main()
