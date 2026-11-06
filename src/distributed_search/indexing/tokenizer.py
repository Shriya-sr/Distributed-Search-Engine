"""
tokenizer.py — Shared tokenizer for indexing AND search.

IMPORTANT: This tokenizer MUST be used identically at index time and at
query time. Never change it without team approval and simultaneous update
to Node 4's search tokenization (contracts.md §D).

Steps:
1. Strip Wikipedia markup: [[...]] link syntax → keep display text only,
   {{...}} template calls → remove entirely.
2. Case-fold to lower-case.
3. Split on non-alphanumeric boundaries.
4. Drop empty tokens.
"""
from __future__ import annotations

import re

# Remove {{template}} blocks entirely (may be nested one level deep)
_TEMPLATE_RE = re.compile(r"\{\{[^{}]*\}\}")
# Convert [[Target|Display]] → Display; [[Target]] → Target
_WIKILINK_RE = re.compile(r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]")
# Split on any non-alphanumeric character
_TOKEN_SPLIT_RE = re.compile(r"[^a-z0-9]+")


def tokenize(text: str) -> list[str]:
    """
    Tokenize a document or query string.

    Returns a list of lowercase alphanumeric tokens with Wikipedia markup
    removed. Empty strings are filtered out.

    Args:
        text: raw document text or query string.

    Returns:
        list of non-empty token strings.

    Examples:
        >>> tokenize("Alpha [[Beta|B]] {{ref}} gamma.")
        ['alpha', 'b', 'gamma']
        >>> tokenize("Hello, World!")
        ['hello', 'world']
    """
    if not text:
        return []
    # Remove templates first (they may contain wikilinks inside)
    text = _TEMPLATE_RE.sub(" ", text)
    # Resolve wikilinks: keep display text
    text = _WIKILINK_RE.sub(r"\1", text)
    # Case fold
    text = text.lower()
    # Split and filter
    tokens = _TOKEN_SPLIT_RE.split(text)
    return [t for t in tokens if t]
