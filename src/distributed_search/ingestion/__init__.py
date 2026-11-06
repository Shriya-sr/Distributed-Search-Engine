"""
Ingestion module — Node 1
Exposes: load_pages_jsonl, load_changes_jsonl, validate_page, validate_change,
         deduplicate_pages, apply_phase1, clean_dangling_links
"""
from .loader import load_pages_jsonl, load_changes_jsonl
from .validator import validate_page, validate_change
from .dedup import deduplicate_pages
from .apply_changes import apply_phase1
from .link_cleaner import clean_dangling_links

__all__ = [
    "load_pages_jsonl",
    "load_changes_jsonl",
    "validate_page",
    "validate_change",
    "deduplicate_pages",
    "apply_phase1",
    "clean_dangling_links",
]
