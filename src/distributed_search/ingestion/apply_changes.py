"""
apply_changes.py — Apply Phase 1 change records to a live document set.

Operation semantics (from contracts.md):
  DELETE : remove doc_id from live set
  UPSERT : insert or fully replace document for that doc_id

The operation is idempotent: applying the same change file twice must
produce the same live document set.
"""
from __future__ import annotations

from .validator import validate_change


def apply_phase1(
    live_docs: dict[str, dict],
    changes: list[dict],
) -> tuple[dict[str, dict], list[dict]]:
    """
    Apply Phase 1 change records to the live document dictionary.

    Args:
        live_docs : dict of doc_id -> document dict (Phase 0 output).
                    This dict is NOT mutated; a new dict is returned.
        changes   : list of raw change records (already loaded, may be
                    invalid — this function validates each one).

    Returns:
        (new_live_docs, rejected)
        new_live_docs — updated live document set after all valid ops
        rejected      — list of dicts with keys 'record' and 'reason'
                        for every change that failed validation
    """
    result = dict(live_docs)  # shallow copy — docs are not mutated
    rejected: list[dict] = []

    for rec in changes:
        ok, reason = validate_change(rec)
        if not ok:
            rejected.append({"record": rec, "reason": reason})
            continue

        op = rec["op"]
        doc_id = rec["doc_id"]

        if op == "DELETE":
            result.pop(doc_id, None)  # idempotent: no-op if already gone
        elif op == "UPSERT":
            result[doc_id] = {
                "doc_id": doc_id,
                "url": rec["url"],
                "title": rec["title"],
                "text": rec["text"],
                "links": list(rec.get("links", [])),
            }

    return result, rejected
