"""
dedup.py — Deduplicate Phase 0 pages by doc_id.

The dataset contains 20 duplicated doc_ids where all copies are identical.
Contract: deduplicate keeping one copy (last-seen is fine since copies are
identical). Do NOT invent version numbers — no 'version' field exists in
the raw data.
"""
from __future__ import annotations


def deduplicate_pages(records: list[dict]) -> dict[str, dict]:
    """
    Deduplicate validated page records by doc_id.

    Args:
        records: list of valid page dicts (already passed validate_page).

    Returns:
        dict mapping doc_id -> page dict (one entry per unique doc_id).
        Since duplicates in the observed dataset are identical, the
        surviving copy is arbitrary.
    """
    seen: dict[str, dict] = {}
    for rec in records:
        doc_id = rec["doc_id"]
        # Last-write wins; identical duplicates make this order-independent.
        seen[doc_id] = rec
    return seen
