
import math

from distributed_search.search.bm25 import compute_bm25


def test_bm25_calculates_score():
    postings = [
        {"term": "apple", "doc_id": "1", "tf": 2},
        {"term": "apple", "doc_id": "2", "tf": 1},
    ]

    doc_stats = [
        {"doc_id": "1", "doc_len": 10},
        {"doc_id": "2", "doc_len": 20},
    ]

    corpus_stats = [
        {"term": "apple", "df": 2},
    ]

    scores = compute_bm25(
        query="apple",
        postings=postings,
        doc_stats=doc_stats,
        corpus_stats=corpus_stats,
        n_docs=2,
        avgdl=15,
    )

    assert scores["1"] > scores["2"]
    assert scores["1"] > 0
    assert scores["2"] > 0


def test_bm25_unknown_term_returns_zero():
    scores = compute_bm25(
        query="unknown",
        postings=[],
        doc_stats=[{"doc_id": "1", "doc_len": 10}],
        corpus_stats=[],
        n_docs=1,
        avgdl=10,
    )

    assert scores == {"1": 0.0}


def test_bm25_empty_query():
    scores = compute_bm25(
        query="",
        postings=[],
        doc_stats=[{"doc_id": "1", "doc_len": 10}],
        corpus_stats=[],
        n_docs=1,
        avgdl=10,
    )

    assert scores == {}
