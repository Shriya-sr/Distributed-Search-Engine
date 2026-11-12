
import pytest

from distributed_search.pagerank.algorithm import compute_pagerank


def test_two_node_cycle():
    graph = {
        "A": ["B"],
        "B": ["A"],
    }

    ranks = compute_pagerank(graph)

    assert ranks["A"] == pytest.approx(0.5)
    assert ranks["B"] == pytest.approx(0.5)


def test_dangling_node():
    graph = {
        "A": ["B"],
        "B": [],
    }

    ranks = compute_pagerank(graph)

    assert sum(ranks.values()) == pytest.approx(1.0)
    assert all(rank > 0 for rank in ranks.values())


def test_missing_links_are_ignored():
    graph = {
        "A": ["MISSING"],
        "B": [],
    }

    ranks = compute_pagerank(graph)

    assert ranks["A"] == pytest.approx(0.5)
    assert ranks["B"] == pytest.approx(0.5)


def test_empty_graph():
    assert compute_pagerank({}) == {}


def test_minimum_iterations():
    with pytest.raises(ValueError):
        compute_pagerank(
            {"A": ["B"], "B": ["A"]},
            iterations=5,
        )


def test_invalid_damping_factor():
    with pytest.raises(ValueError):
        compute_pagerank(
            {"A": []},
            damping_factor=1.5,
        )
