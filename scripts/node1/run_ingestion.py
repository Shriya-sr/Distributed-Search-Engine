#!/usr/bin/env python3
"""
scripts/node1/run_ingestion.py
Node 1 ingestion runner — executes Phase 0 or Phase 1 ingestion.

Usage (on VM1):
    python scripts/node1/run_ingestion.py --phase 0 --pages data/pages.jsonl
    python scripts/node1/run_ingestion.py --phase 1 --changes data/changes.json

Environment variables (keep actual values in .env, NOT in Git):
    SPARK_MASTER_URL   e.g. spark://192.168.x.x:7077   (Node 3's ZeroTier IP)
    ICEBERG_WAREHOUSE  e.g. hdfs://namenode:9000/warehouse

This script wires together the pure-Python ingestion logic with a Spark
write layer. The ingestion functions are fully testable without Spark.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Make the src package importable when run directly from the repo root.
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from distributed_search.ingestion import (
    load_pages_jsonl,
    load_changes_jsonl,
    validate_page,
    validate_change,
    deduplicate_pages,
    apply_phase1,
    clean_dangling_links,
)


def run_phase0(pages_path: str) -> dict[str, dict]:
    print(f"[Phase 0] Loading pages from {pages_path} ...")
    records, bad = load_pages_jsonl(pages_path)
    print(f"  Parsed {len(records)} records, quarantined {bad} malformed lines.")

    valid, invalid = [], []
    for rec in records:
        ok, reason = validate_page(rec)
        if ok:
            valid.append(rec)
        else:
            invalid.append((rec, reason))
    print(f"  Valid: {len(valid)}, Invalid (rejected): {len(invalid)}")

    live_docs = deduplicate_pages(valid)
    print(f"  After dedup: {len(live_docs)} unique documents.")
    return live_docs


def run_phase1(live_docs: dict[str, dict], changes_path: str) -> dict[str, dict]:
    print(f"[Phase 1] Loading changes from {changes_path} ...")
    records, bad = load_changes_jsonl(changes_path)
    print(f"  Parsed {len(records)} change records, quarantined {bad} malformed lines.")

    new_live, rejected = apply_phase1(live_docs, records)
    print(f"  After apply: {len(new_live)} live documents, {len(rejected)} rejected changes.")

    cleaned = clean_dangling_links(new_live)
    total_links = sum(len(d.get("links", [])) for d in cleaned.values())
    print(f"  After link cleanup: {total_links} total links remain.")
    return cleaned


def write_to_iceberg(live_docs: dict[str, dict], phase: int) -> None:
    """
    Stub: replace with real PySpark + Iceberg write logic once the cluster
    is live. Keep ingestion logic and Spark I/O separate so unit tests
    never need a Spark session.
    """
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "(not set)")
    master = os.environ.get("SPARK_MASTER_URL", "(not set)")
    print(f"[Iceberg] Would write {len(live_docs)} docs to warehouse={warehouse}")
    print(f"          Using Spark master: {master}")
    print("          (Spark write not yet implemented — integrate after cluster setup)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Node 1 ingestion runner")
    parser.add_argument("--phase", type=int, choices=[0, 1], required=True,
                        help="Ingestion phase: 0 for pages.jsonl, 1 for changes")
    parser.add_argument("--pages", default="data/pages.jsonl",
                        help="Path to Phase 0 pages JSONL file")
    parser.add_argument("--changes", default="data/changes.json",
                        help="Path to Phase 1 changes JSONL file")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and validate only; do not write to Iceberg")
    args = parser.parse_args()

    live_docs: dict[str, dict] = {}

    if args.phase == 0:
        live_docs = run_phase0(args.pages)
    elif args.phase == 1:
        # Phase 1 requires Phase 0 to have been ingested; in real run we'd
        # read from Iceberg. For now load Phase 0 fresh then apply changes.
        live_docs = run_phase0(args.pages)
        live_docs = run_phase1(live_docs, args.changes)

    if not args.dry_run:
        write_to_iceberg(live_docs, args.phase)
    else:
        print("[dry-run] Skipping Iceberg write.")

    print("Done.")


if __name__ == "__main__":
    main()
