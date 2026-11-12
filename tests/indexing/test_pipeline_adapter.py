from distributed_search.export.index_adapter import (
    normalize_records,
    resolve_pagerank_links,
    validate_index_statistics,
)


def test_postings_conversion():
    records = [
        {"term": "spark", "doc_id": 123, "tf": 2},
    ]
    assert normalize_records("postings", records) == [
        {"term": "spark", "doc_id": "123", "tf": 2},
    ]


def test_document_statistics_conversion():
    records = [{"doc_id": 1, "doc_len": 10}]
    assert normalize_records("doc_stats", records) == [
        {"doc_id": "1", "doc_len": 10}
    ]


def test_term_statistics_conversion():
    records = [{"term": "spark", "df": 2}]
    assert normalize_records("term_stats", records) == [
        {"term": "spark", "df": 2}
    ]


def test_pagerank_url_resolution():
    documents = [
        {
            "doc_id": "1",
            "url": "https://a.com",
            "links": ["https://b.com"],
        },
        {
            "doc_id": "2",
            "url": "https://b.com",
            "links": [],
        },
    ]
    assert resolve_pagerank_links(documents) == [
        ("1", ["2"]),
        ("2", []),
    ]


def test_preserve_zero_average_length():
    assert validate_index_statistics(
        {"N": 2, "avgdl": 0.0}
    ) == {"N": 2, "avgdl": 0.0}
