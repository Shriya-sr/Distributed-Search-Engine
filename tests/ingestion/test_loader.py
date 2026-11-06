"""tests/ingestion/test_loader.py"""
import pytest
from pathlib import Path
from distributed_search.ingestion import load_pages_jsonl, load_changes_jsonl

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_load_pages_counts_valid_records():
    records, bad = load_pages_jsonl(FIXTURES / "toy_pages.jsonl")
    # 7 non-empty lines: 5 valid + 1 dup + 1 malformed
    assert len(records) == 6  # all JSON-parseable lines (5 unique + 1 dup)
    assert bad == 1            # 1 malformed line quarantined


def test_load_pages_quarantine_does_not_raise():
    """A bad line must NOT abort the entire load."""
    records, bad = load_pages_jsonl(FIXTURES / "toy_pages.jsonl")
    assert bad >= 1


def test_load_changes_reads_jsonlines():
    records, bad = load_changes_jsonl(FIXTURES / "toy_changes.jsonl")
    assert len(records) == 4   # 2 DELETE + 2 UPSERT
    assert bad == 0


def test_load_changes_ops_present():
    records, _ = load_changes_jsonl(FIXTURES / "toy_changes.jsonl")
    ops = {r["op"] for r in records}
    assert ops == {"DELETE", "UPSERT"}
