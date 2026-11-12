import pytest

from distributed_search.search.bm25 import compute_bm25


def score(n_docs, avgdl, postings=(), doc_stats=(), term_stats=()):
    return compute_bm25(
        "spark",
        postings,
        doc_stats,
        term_stats,
        n_docs=n_docs,
        avgdl=avgdl,
    )


def test_no_documents():
    assert score(0, 0.0) == {}


def test_all_documents_empty():
    assert score(
        2,
        0.0,
        doc_stats=[
            {"doc_id": "1", "doc_len": 0},
            {"doc_id": "2", "doc_len": 0},
        ],
    ) == {"1": 0.0, "2": 0.0}


def test_zero_avgdl_rejects_nonempty_postings():
    with pytest.raises(ValueError):
        score(
            1,
            0.0,
            postings=[{"term": "spark", "doc_id": "1", "tf": 1}],
            doc_stats=[{"doc_id": "1", "doc_len": 0}],
        )


def test_negative_avgdl_rejected():
    with pytest.raises(ValueError):
        score(1, -1.0)


def test_normal_bm25_still_works():
    result = score(
        1,
        2.0,
        postings=[{"term": "spark", "doc_id": "1", "tf": 2}],
        doc_stats=[{"doc_id": "1", "doc_len": 2}],
        term_stats=[{"term": "spark", "df": 1}],
    )
    assert result["1"] > 0
