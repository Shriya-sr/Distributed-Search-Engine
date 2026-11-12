
"""BM25 scoring for the distributed search engine."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence

from distributed_search.indexing.tokenizer import tokenize


def compute_bm25(
    query: str,
    postings: Sequence[Mapping],
    doc_stats: Sequence[Mapping],
    corpus_stats: Sequence[Mapping],
    n_docs: int,
    avgdl: float,
    k1: float = 1.2,
    b: float = 0.75,
) -> dict[str, float]:
    """
    Calculate BM25 relevance scores for documents matching a query.

    Args:
        query: Raw user search query.
        postings: Rows containing term, doc_id and tf.
        doc_stats: Rows containing doc_id and doc_len.
        corpus_stats: Rows containing term and df.
        n_docs: Total number of documents.
        avgdl: Average document length.
        k1: Term frequency saturation parameter.
        b: Document length normalization parameter.

    Returns:
        Dictionary mapping document IDs to BM25 scores.
    """
    if k1 < 0:
        raise ValueError("k1 must be non-negative")

    if not 0 <= b <= 1:
        raise ValueError("b must be between 0 and 1")

    if n_docs < 0:
        raise ValueError("n_docs cannot be negative")

    if avgdl <= 0:
        raise ValueError("avgdl must be positive")

    if n_docs == 0:
        return {}

    query_terms = tokenize(query)

    if not query_terms:
        return {}

    # Document lengths indexed by document ID.
    doc_lengths = {
        str(row["doc_id"]): int(row["doc_len"])
        for row in doc_stats
    }

    # Initialize scores for every document.
    scores = {doc_id: 0.0 for doc_id in doc_lengths}

    # Document frequency indexed by term.
    term_df = {
        row["term"]: int(row["df"])
        for row in corpus_stats
    }

    # Group postings by term.
    term_postings = defaultdict(list)

    for row in postings:
        term_postings[row["term"]].append(row)

    # Calculate BM25.
    for term in query_terms:

        df = term_df.get(term, 0)

        if df <= 0:
            continue

        if df > n_docs:
            raise ValueError(f"Invalid document frequency for {term}")

        idf = math.log(
            1.0 + (n_docs - df + 0.5) / (df + 0.5)
        )

        for row in term_postings.get(term, []):

            doc_id = str(row["doc_id"])

            if doc_id not in doc_lengths:
                raise ValueError(
                    f"Missing document length for {doc_id}"
                )

            tf = float(row["tf"])
            doc_len = doc_lengths[doc_id]

            if tf <= 0 or doc_len < 0:
                raise ValueError("Invalid term frequency or document length")

            numerator = tf * (k1 + 1.0)

            denominator = tf + k1 * (
                1.0 - b + b * (doc_len / avgdl)
            )

            if denominator == 0:
                continue

            scores[doc_id] += idf * (
                numerator / denominator
            )

    return scores
