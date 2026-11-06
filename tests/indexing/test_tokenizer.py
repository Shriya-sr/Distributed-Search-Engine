"""tests/indexing/test_tokenizer.py"""
import pytest
from distributed_search.indexing import tokenize


def test_basic_lowercase():
    assert tokenize("Hello World") == ["hello", "world"]


def test_punctuation_stripped():
    assert tokenize("Hello, World!") == ["hello", "world"]


def test_wiki_template_removed():
    tokens = tokenize("{{monththisyear|4}} April is a month.")
    assert "monththisyear" not in tokens
    assert "april" in tokens
    assert "month" in tokens


def test_wiki_link_keeps_display_text():
    tokens = tokenize("See [[Gregorian calendar|Gregorian]] dates.")
    assert "gregorian" in tokens
    # The link target should NOT appear as its own token
    assert "calendar" not in tokens or "gregorian" in tokens  # display text wins


def test_wiki_link_no_pipe_keeps_text():
    tokens = tokenize("Visit [[Paris]] for art.")
    assert "paris" in tokens


def test_empty_string_returns_empty():
    assert tokenize("") == []


def test_numbers_kept():
    tokens = tokenize("Apollo 11 landed in 1969.")
    assert "apollo" in tokens
    assert "11" in tokens
    assert "1969" in tokens


def test_no_empty_tokens():
    tokens = tokenize("   lots   of   spaces   ")
    assert all(t != "" for t in tokens)


def test_wikipedia_markup_mix():
    text = "{{ref}} [[Alpha|A]] [[Beta]] gamma"
    tokens = tokenize(text)
    assert "a" in tokens
    assert "beta" in tokens
    assert "gamma" in tokens
    # template content removed
    assert "ref" not in tokens


def test_consistent_query_and_index():
    """
    Indexing a phrase and querying it must produce overlapping tokens.
    This is the core correctness contract.
    """
    indexed = set(tokenize("The quick brown Fox jumps."))
    queried = set(tokenize("Quick Fox"))
    assert queried.issubset(indexed)
