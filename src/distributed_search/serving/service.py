
"""Search service combining BM25 and precomputed PageRank."""

from typing import Any

from distributed_search.search.bm25 import compute_bm25
from distributed_search.search.ranking import combine_scores


class SearchService:
    """Perform local searches over a loaded Parquet export."""

    def __init__(self, serving_data: dict[str, Any]) -> None:
        self.version = serving_data["version"]
        self.corpus_stats = serving_data["corpus_stats"]

        self.postings = serving_data["postings"]
        self.documents = serving_data["documents"]
        self.document_stats = serving_data["document_stats"]
        self.term_stats = serving_data["term_stats"]
        self.pagerank = serving_data["pagerank"]

        self.n_docs = self.corpus_stats["N"]
        self.avgdl = self.corpus_stats["avgdl"]

        self.documents_by_id = {
            str(document["doc_id"]): document
            for document in self.documents
        }

        self.pagerank_by_id = {
            str(record["doc_id"]): record["pagerank"]
            for record in self.pagerank
        }

    def search(self, query: str, limit: int = 10) -> dict[str, Any]:
        """Return documents ranked by combined BM25 and PageRank scores."""

        if not isinstance(query, str) or not query.strip():
            raise ValueError("Search query must not be empty")

        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("Limit must be a positive integer")

        bm25_scores = compute_bm25(
            query=query,
            postings=self.postings,
            doc_stats=self.document_stats,
            corpus_stats=self.term_stats,
            n_docs=self.n_docs,
            avgdl=self.avgdl,
        )

        # Keep only documents that match the search query.
        matched_bm25 = {
            doc_id: score
            for doc_id, score in bm25_scores.items()
            if score > 0
        }

        if not matched_bm25:
            return {
                "query": query,
                "version": self.version,
                "total_results": 0,
                "results": [],
            }

        # Retrieve PageRank scores for matching documents only.
        matched_pagerank = {
            doc_id: self.pagerank_by_id.get(doc_id, 0.0)
            for doc_id in matched_bm25
        }

        # Combine normalized BM25 and PageRank scores.
        combined = combine_scores(
            bm25_scores=matched_bm25,
            pagerank_scores=matched_pagerank,
            bm25_weight=0.7,
            pagerank_weight=0.3,
        )

        ranked_results = []

        for doc_id, score in combined.items():
            document = self.documents_by_id.get(doc_id)

            if document is None:
                continue

            ranked_results.append({
                "doc_id": doc_id,
                "title": document.get("title", ""),
                "url": document.get("url", ""),
                "score": score,
                "version": self.version,
            })

        # Highest-scoring documents appear first.
        ranked_results.sort(
            key=lambda result: result["score"],
            reverse=True,
        )

        return {
            "query": query,
            "version": self.version,
            "total_results": len(ranked_results),
            "results": ranked_results[:limit],
        }
