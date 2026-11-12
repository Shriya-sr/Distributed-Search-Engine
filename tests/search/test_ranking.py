
import pytest

from distributed_search.search.ranking import (
    normalize_scores,
    combine_scores,
)


def test_normalize_scores():
    scores = {"A": 10.0, "B": 20.0, "C": 30.0}

    normalized = normalize_scores(scores)

    assert normalized == {
        "A": 0.0,
        "B": 0.5,
        "C": 1.0,
    }


def test_normalize_equal_scores():
    scores = {"A": 5.0, "B": 5.0}

    assert normalize_scores(scores) == {
        "A": 0.0,
        "B": 0.0,
    }


def test_combine_scores():
    bm25 = {"A": 10.0, "B": 20.0}
    pagerank = {"A": 0.2, "B": 0.8}

    combined = combine_scores(bm25, pagerank)

    assert combined["A"] == pytest.approx(0.0)
    assert combined["B"] == pytest.approx(1.0)


def test_invalid_weights():
    with pytest.raises(ValueError):
        combine_scores(
            {"A": 1.0},
            {"A": 0.5},
            bm25_weight=0.8,
            pagerank_weight=0.3,
        )


def test_empty_bm25_scores():
    assert combine_scores({}, {"A": 0.5}) == {}
