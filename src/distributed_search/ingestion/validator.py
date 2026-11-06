"""
validator.py — Field-level validation for Phase 0 pages and Phase 1 changes.

Rules (from contracts.md):
  Phase 0 page  : must have doc_id, url, title, text (strings); links optional (default [])
  Phase 1 DELETE: must have doc_id, op == "DELETE"; no other fields required
  Phase 1 UPSERT: must have doc_id, op == "DELETE"|"UPSERT", url, title, text; links optional
"""
from __future__ import annotations

_REQUIRED_PAGE_FIELDS = {"doc_id", "url", "title", "text"}
_REQUIRED_UPSERT_FIELDS = {"doc_id", "op", "url", "title", "text"}
_VALID_OPS = {"DELETE", "UPSERT"}


def validate_page(record: dict) -> tuple[bool, str]:
    """
    Validate a Phase 0 page record.

    Returns (is_valid, reason). reason is "" when valid.
    """
    missing = _REQUIRED_PAGE_FIELDS - record.keys()
    if missing:
        return False, f"missing fields: {missing}"
    for f in _REQUIRED_PAGE_FIELDS:
        if not isinstance(record[f], str):
            return False, f"field '{f}' must be a string"
    links = record.get("links", [])
    if not isinstance(links, list):
        return False, "'links' must be a list when present"
    return True, ""


def validate_change(record: dict) -> tuple[bool, str]:
    """
    Validate a Phase 1 change record (DELETE or UPSERT).

    Returns (is_valid, reason).
    """
    if "doc_id" not in record:
        return False, "missing 'doc_id'"
    if not isinstance(record["doc_id"], str):
        return False, "'doc_id' must be a string"
    op = record.get("op")
    if op not in _VALID_OPS:
        return False, f"'op' must be one of {_VALID_OPS}, got {op!r}"
    if op == "UPSERT":
        missing = _REQUIRED_UPSERT_FIELDS - record.keys()
        if missing:
            return False, f"UPSERT missing fields: {missing}"
        for f in {"url", "title", "text"}:
            if not isinstance(record[f], str):
                return False, f"UPSERT field '{f}' must be a string"
        links = record.get("links", [])
        if not isinstance(links, list):
            return False, "'links' must be a list when present"
    return True, ""
