"""Convert distributed index records to serving export records."""

INDEX_TO_SERVING = {
    "postings": "postings",
    "doc_stats": "document_stats",
    "term_stats": "term_stats",
}


def serving_dataset_name(name: str) -> str:
    try:
        return INDEX_TO_SERVING[name]
    except KeyError as exc:
        raise ValueError(f"Unknown index dataset: {name}") from exc


def validate_index_statistics(corpus_stats: dict) -> dict:
    """Preserve true corpus statistics; reject invalid values."""
    import math

    n = corpus_stats["N"]
    avgdl = corpus_stats["avgdl"]

    if isinstance(n, bool) or not isinstance(n, int) or n < 0:
        raise ValueError("N must be a nonnegative integer")

    if (
        isinstance(avgdl, bool)
        or not isinstance(avgdl, (int, float))
        or not math.isfinite(avgdl)
        or avgdl < 0
    ):
        raise ValueError("avgdl must be finite and nonnegative")

    return {"N": n, "avgdl": float(avgdl)}


def normalize_records(dataset_name: str, records: list[dict]) -> list[dict]:
    """Convert Spark index records to the serving export contract."""
    name = serving_dataset_name(dataset_name)

    fields = {
        "postings": ("term", "doc_id", "tf"),
        "document_stats": ("doc_id", "doc_len"),
        "term_stats": ("term", "df"),
    }[name]

    integer_fields = {"tf", "doc_len", "df"}

    return [
        {
            field: (
                int(record[field])
                if field in integer_fields
                else str(record[field])
            )
            for field in fields
        }
        for record in records
    ]


def resolve_pagerank_links(documents: list[dict]) -> list[tuple]:
    """Map outgoing document URLs to IDs for the PageRank algorithm."""
    url_to_id = {doc["url"]: doc["doc_id"] for doc in documents}

    return [
        (
            doc["doc_id"],
            sorted({
                url_to_id[url]
                for url in (doc.get("links") or [])
                if url in url_to_id
            }),
        )
        for doc in documents
    ]
