import pytest

from distributed_search.export.index_adapter import (
    serving_dataset_name,
    validate_index_statistics,
)


def test_dataset_names():
    assert serving_dataset_name("postings") == "postings"
    assert serving_dataset_name("doc_stats") == "document_stats"
    assert serving_dataset_name("term_stats") == "term_stats"


def test_unknown_dataset():
    with pytest.raises(ValueError):
        serving_dataset_name("unknown")


def test_corpus_statistics():
    assert validate_index_statistics(
        {"N": 2, "avgdl": 4.5}
    ) == {"N": 2, "avgdl": 4.5}


def test_empty_document_corpus():
    assert validate_index_statistics(
        {"N": 3, "avgdl": 0.0}
    ) == {"N": 3, "avgdl": 0.0}
