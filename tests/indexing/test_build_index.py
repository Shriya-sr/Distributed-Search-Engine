"""tests/indexing/test_build_index.py"""
import pytest
from distributed_search.indexing import (
    build_postings,
    build_doc_stats,
    build_corpus_stats,
)


# ── Toy corpus ─────────────────────────────────────────────────────────────
# After tokenization (lower, no punctuation):
#   doc "A": alpha beta gamma delta        → 4 tokens
#   doc "B": beta gamma gamma epsilon      → 4 tokens (gamma tf=2)
#   doc "C": gamma delta delta delta       → 4 tokens (delta tf=3)

TOY_DOCS = {
    "A": {"doc_id": "A", "url": "u", "title": "t", "text": "alpha beta gamma delta", "links": []},
    "B": {"doc_id": "B", "url": "u", "title": "t", "text": "beta gamma gamma epsilon", "links": []},
    "C": {"doc_id": "C", "url": "u", "title": "t", "text": "gamma delta delta delta", "links": []},
}


# ── build_postings ─────────────────────────────────────────────────────────

def test_postings_has_correct_structure():
    postings = build_postings(TOY_DOCS)
    for row in postings:
        assert "term" in row
        assert "doc_id" in row
        assert "tf" in row


def test_postings_tf_counts():
    postings = build_postings(TOY_DOCS)
    idx = {(r["term"], r["doc_id"]): r["tf"] for r in postings}
    assert idx[("gamma", "A")] == 1
    assert idx[("gamma", "B")] == 2   # appears twice in doc B
    assert idx[("delta", "C")] == 3   # appears three times in doc C


def test_postings_sorted():
    postings = build_postings(TOY_DOCS)
    keys = [(r["term"], r["doc_id"]) for r in postings]
    assert keys == sorted(keys)


def test_postings_no_cross_doc_pollution():
    postings = build_postings(TOY_DOCS)
    # "alpha" only appears in doc A
    alpha_rows = [r for r in postings if r["term"] == "alpha"]
    assert all(r["doc_id"] == "A" for r in alpha_rows)


# ── build_doc_stats ────────────────────────────────────────────────────────

def test_doc_stats_has_correct_structure():
    stats = build_doc_stats(TOY_DOCS)
    for row in stats:
        assert "doc_id" in row
        assert "doc_len" in row


def test_doc_stats_lengths():
    stats = build_doc_stats(TOY_DOCS)
    idx = {r["doc_id"]: r["doc_len"] for r in stats}
    assert idx["A"] == 4
    assert idx["B"] == 4   # "gamma" counted twice
    assert idx["C"] == 4   # "delta" counted three times; total=4 tokens


def test_doc_stats_sorted():
    stats = build_doc_stats(TOY_DOCS)
    ids = [r["doc_id"] for r in stats]
    assert ids == sorted(ids)


# ── build_corpus_stats ─────────────────────────────────────────────────────

def test_corpus_stats_structure():
    postings = build_postings(TOY_DOCS)
    doc_stats = build_doc_stats(TOY_DOCS)
    term_df, N, avgdl = build_corpus_stats(postings, doc_stats)
    for row in term_df:
        assert "term" in row
        assert "df" in row


def test_corpus_N():
    postings = build_postings(TOY_DOCS)
    doc_stats = build_doc_stats(TOY_DOCS)
    _, N, _ = build_corpus_stats(postings, doc_stats)
    assert N == 3


def test_corpus_avgdl():
    postings = build_postings(TOY_DOCS)
    doc_stats = build_doc_stats(TOY_DOCS)
    _, _, avgdl = build_corpus_stats(postings, doc_stats)
    # (4 + 4 + 4) / 3 = 4.0
    assert avgdl == pytest.approx(4.0)


def test_corpus_df_gamma():
    """'gamma' appears in all 3 docs → df=3."""
    postings = build_postings(TOY_DOCS)
    doc_stats = build_doc_stats(TOY_DOCS)
    term_df, _, _ = build_corpus_stats(postings, doc_stats)
    idx = {r["term"]: r["df"] for r in term_df}
    assert idx["gamma"] == 3


def test_corpus_df_alpha():
    """'alpha' appears only in doc A → df=1."""
    postings = build_postings(TOY_DOCS)
    doc_stats = build_doc_stats(TOY_DOCS)
    term_df, _, _ = build_corpus_stats(postings, doc_stats)
    idx = {r["term"]: r["df"] for r in term_df}
    assert idx["alpha"] == 1


def test_corpus_df_sorted():
    postings = build_postings(TOY_DOCS)
    doc_stats = build_doc_stats(TOY_DOCS)
    term_df, _, _ = build_corpus_stats(postings, doc_stats)
    terms = [r["term"] for r in term_df]
    assert terms == sorted(terms)


def test_avgdl_minimum_one_on_empty():
    """Corner case: empty corpus must not divide by zero."""
    _, N, avgdl = build_corpus_stats([], [])
    assert N == 0
    assert avgdl == 1.0
