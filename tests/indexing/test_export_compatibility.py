from distributed_search.export.index_adapter import normalize_records
from distributed_search.export.parquet_export import export_parquet
from distributed_search.serving.loader import load_serving_data


def test_index_to_serving_export(tmp_path):
    postings = normalize_records(
        "postings",
        [{"term": "spark", "doc_id": 1, "tf": 2}],
    )
    document_stats = normalize_records(
        "doc_stats",
        [{"doc_id": 1, "doc_len": 2}],
    )
    term_stats = normalize_records(
        "term_stats",
        [{"term": "spark", "df": 1}],
    )

    export_dir = export_parquet(
        version=0,
        output_dir=tmp_path,
        postings=postings,
        documents=[
            {
                "doc_id": "1",
                "url": "https://example.com",
                "title": "Spark Example",
                "text": "spark spark",
                "links": [],
            }
        ],
        document_stats=document_stats,
        term_stats=term_stats,
        pagerank=[{"doc_id": "1", "pagerank": 1.0}],
        corpus_stats={"N": 1, "avgdl": 2.0},
    )

    loaded = load_serving_data(export_dir)

    assert loaded["version"] == 0
    assert loaded["postings"] == postings
    assert loaded["document_stats"] == document_stats
    assert loaded["term_stats"] == term_stats
    assert loaded["pagerank"] == [
        {"doc_id": "1", "pagerank": 1.0}
    ]
    assert loaded["corpus_stats"] == {"N": 1, "avgdl": 2.0}
