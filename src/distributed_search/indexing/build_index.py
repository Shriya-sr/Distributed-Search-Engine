"""
build_index.py — Pure-Python inverted index builder.

Runs against a live_docs dict (output of ingestion pipeline).
In production, these functions are wrapped by a Spark driver that runs on
Node 3; the core logic lives here so it can be tested without Spark.

Output schemas (from contracts.md §D):
  postings     : list of {'term': str, 'doc_id': str, 'tf': int}
  doc_stats    : list of {'doc_id': str, 'doc_len': int}
  corpus_stats : list of {'term': str, 'df': int}
                 plus corpus-level scalars N (int) and avgdl (float)
"""
from __future__ import annotations

from collections import Counter, defaultdict

from .tokenizer import tokenize


def build_postings(live_docs: dict[str, dict]) -> list[dict]:
    """
    Build the inverted index postings list.

    For each document, tokenize its text and count term frequencies.

    Args:
        live_docs: doc_id -> document dict (must have 'text' field).

    Returns:
        List of dicts: {'term': str, 'doc_id': str, 'tf': int}
        Sorted by term then doc_id for deterministic output.
    """
    postings: list[dict] = []
    for doc_id, doc in live_docs.items():
        tokens = tokenize(doc.get("text", ""))
        counts = Counter(tokens)
        for term, tf in counts.items():
            postings.append({"term": term, "doc_id": doc_id, "tf": tf})
    postings.sort(key=lambda r: (r["term"], r["doc_id"]))
    return postings


def build_doc_stats(live_docs: dict[str, dict]) -> list[dict]:
    """
    Compute per-document length statistics.

    doc_len = number of tokens in the document's text field.

    Args:
        live_docs: doc_id -> document dict.

    Returns:
        List of dicts: {'doc_id': str, 'doc_len': int}
        Sorted by doc_id for deterministic output.
    """
    stats: list[dict] = []
    for doc_id, doc in live_docs.items():
        tokens = tokenize(doc.get("text", ""))
        stats.append({"doc_id": doc_id, "doc_len": len(tokens)})
    stats.sort(key=lambda r: r["doc_id"])
    return stats


def build_corpus_stats(
    postings: list[dict],
    doc_stats: list[dict],
) -> tuple[list[dict], int, float]:
    """
    Compute corpus-level statistics for BM25.

    Args:
        postings : output of build_postings()
        doc_stats: output of build_doc_stats()

    Returns:
        (term_df_list, N, avgdl)
        term_df_list — list of {'term': str, 'df': int} sorted by term
        N            — total number of documents in the corpus
        avgdl        — average document length (tokens); 1.0 minimum
    """
    # Document frequency: count distinct doc_ids per term
    term_docs: dict[str, set] = defaultdict(set)
    for row in postings:
        term_docs[row["term"]].add(row["doc_id"])

    term_df_list = [
        {"term": term, "df": len(doc_ids)}
        for term, doc_ids in term_docs.items()
    ]
    term_df_list.sort(key=lambda r: r["term"])

    # Corpus-level scalars
    N = len(doc_stats)
    total_len = sum(r["doc_len"] for r in doc_stats)
    avgdl = max(total_len / N, 1.0) if N > 0 else 1.0

    return term_df_list, N, avgdl
