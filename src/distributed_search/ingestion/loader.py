"""
loader.py — Parse JSONL files for Phase 0 (pages) and Phase 1 (changes).

Malformed JSON lines are quarantined (counted and skipped) rather than
aborting the whole load — matching the contract requirement.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator


def load_pages_jsonl(path: str | Path) -> tuple[list[dict], int]:
    """
    Load a pages JSONL file (Phase 0).

    Returns:
        (records, quarantine_count)
        records         — list of raw dicts that parsed successfully
        quarantine_count — number of lines that failed JSON parsing
    """
    records: list[dict] = []
    quarantine_count = 0
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                quarantine_count += 1
    return records, quarantine_count


def load_changes_jsonl(path: str | Path) -> tuple[list[dict], int]:
    """
    Load a changes file (Phase 1).

    The file is named changes.json but contains JSON Lines, not a JSON
    array — this matches the confirmed contract.

    Returns:
        (records, quarantine_count)
    """
    # Reuse the same line-by-line parser; contract is identical.
    return load_pages_jsonl(path)


def _iter_jsonl(path: str | Path) -> Iterator[dict]:
    """Internal helper: yield successfully parsed records."""
    records, _ = load_pages_jsonl(path)
    yield from records
