"""
link_cleaner.py — Remove links that point to non-existent doc_ids.

After Phase 1, some links target deleted or never-introduced doc_ids.
This step must be performed against the FINAL live document set, not
the Phase 0 ID set (per contracts.md).
"""
from __future__ import annotations


def clean_dangling_links(live_docs: dict[str, dict]) -> dict[str, dict]:
    """
    Return a new live_docs dict with all dangling links removed.

    A link is dangling if the target doc_id is not in live_docs.
    The original dicts are not mutated.

    Args:
        live_docs: dict of doc_id -> document dict (after apply_phase1).

    Returns:
        New dict with the same structure; each doc's 'links' list
        contains only IDs that exist in live_docs.
    """
    valid_ids = set(live_docs.keys())
    cleaned: dict[str, dict] = {}
    for doc_id, doc in live_docs.items():
        raw_links = doc.get("links", [])
        good_links = [lnk for lnk in raw_links if lnk in valid_ids]
        cleaned[doc_id] = {**doc, "links": good_links}
    return cleaned
