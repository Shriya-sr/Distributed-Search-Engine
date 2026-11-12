from unittest.mock import MagicMock, patch

from distributed_search.export.pipeline import export_pipeline


def fake_frame(rows, columns=None):
    frame = MagicMock()
    frame.columns = columns or []
    frame.select.return_value = frame
    frame.limit.return_value = frame
    frame.collect.return_value = [
        MagicMock(asDict=lambda recursive=True, row=row: row)
        for row in rows
    ]
    return frame


@patch("distributed_search.export.pipeline.export_parquet")
@patch("distributed_search.export.pipeline.compute_pagerank_rdd")
def test_pipeline_wiring(mock_pagerank, mock_export):
    spark = MagicMock()

    documents = [{
        "doc_id": "1",
        "url": "https://example.com",
        "title": "Spark",
        "text": "spark spark",
        "links": [],
    }]

    pages = fake_frame(
        documents,
        ["doc_id", "url", "title", "text", "links"],
    )

    postings = fake_frame([
        {"term": "spark", "doc_id": "1", "tf": 2}
    ])
    doc_stats = fake_frame([
        {"doc_id": "1", "doc_len": 2}
    ])
    term_stats = fake_frame([
        {"term": "spark", "df": 1}
    ])
    corpus_stats = fake_frame([
        {"N": 1, "avgdl": 2.0}
    ])

    spark.read.option.return_value.format.return_value.load.return_value = pages

    index_metadata = fake_frame([
        {"snapshot_id": "12345"}
    ])

    spark.read.parquet.side_effect = [
        index_metadata,
        postings,
        doc_stats,
        term_stats,
        corpus_stats,
    ]

    # Spark DataFrame operations now build the PageRank graph.
    # Mock the resulting RDD without requiring a live Spark cluster.
    pages.select.return_value = pages
    pages.distinct.return_value = pages
    pages.join.return_value = pages
    pages.groupBy.return_value.agg.return_value = pages
    pages.rdd.map.return_value = "graph-rdd"

    mock_pagerank.return_value.collect.return_value = [("1", 1.0)]
    mock_export.return_value = "/tmp/serving/v0"

    with patch.dict(
        "sys.modules",
        {"pyspark.sql.functions": MagicMock()},
    ):
        result = export_pipeline(
            spark,
            version=0,
            snapshot_id=12345,
        )

    assert result == "/tmp/serving/v0"

    spark.read.option.assert_called_once_with(
        "snapshot-id", "12345"
    )

    assert spark.read.parquet.call_count == 5

    mock_pagerank.assert_called_once_with(
        "graph-rdd",
        iterations=10,
        damping_factor=0.85,
    )

    kwargs = mock_export.call_args.kwargs

    assert kwargs["version"] == 0
    assert kwargs["corpus_stats"] == {"N": 1, "avgdl": 2.0}
    assert kwargs["postings"] == [
        {"term": "spark", "doc_id": "1", "tf": 2}
    ]
    assert kwargs["document_stats"] == [
        {"doc_id": "1", "doc_len": 2}
    ]
    assert kwargs["pagerank"] == [
        {"doc_id": "1", "pagerank": 1.0}
    ]


def test_pipeline_requires_snapshot():
    spark = MagicMock()

    try:
        export_pipeline(
            spark,
            version=0,
            snapshot_id=None,
        )
    except ValueError as exc:
        assert "snapshot" in str(exc).lower()
    else:
        raise AssertionError("Missing snapshot ID was accepted")
