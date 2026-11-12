
"""Score normalization and BM25-PageRank combination."""

from collections.abc import Mapping


def normalize_scores(scores: Mapping[str, float]) -> dict[str, float]:
    """Normalize scores to the range [0, 1] using min-max normalization."""
    if not scores:
        return {}

    values = list(scores.values())
    minimum = min(values)
    maximum = max(values)

    if maximum == minimum:
        return {doc_id: 0.0 for doc_id in scores}

    return {
        doc_id: (score - minimum) / (maximum - minimum)
        for doc_id, score in scores.items()
    }


def combine_scores(
    bm25_scores: Mapping[str, float],
    pagerank_scores: Mapping[str, float],
    bm25_weight: float = 0.7,
    pagerank_weight: float = 0.3,
) -> dict[str, float]:
    """Combine normalized BM25 and PageRank scores."""
    if bm25_weight < 0 or pagerank_weight < 0:
        raise ValueError("Weights must be non-negative")

    if not bm25_scores:
        return {}

    if not pagerank_scores:
        raise ValueError("PageRank scores cannot be empty")

    if not abs(bm25_weight + pagerank_weight - 1.0) < 1e-9:
        raise ValueError("Weights must sum to 1")

    normalized_bm25 = normalize_scores(bm25_scores)
    normalized_pagerank = normalize_scores(pagerank_scores)

    doc_ids = set(bm25_scores) | set(pagerank_scores)

    combined = {}

    for doc_id in doc_ids:
        bm25 = normalized_bm25.get(doc_id, 0.0)
        pagerank = normalized_pagerank.get(doc_id, 0.0)

        combined[doc_id] = (
            bm25_weight * bm25
            + pagerank_weight * pagerank
        )

    return combined
