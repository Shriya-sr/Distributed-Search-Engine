import json

import pyarrow.parquet as pq
import pytest

from distributed_search.export import export_parquet, validate_export


@pytest.fixture
def export_data():
    return {
        "postings": [
            {"term": "search", "doc_id": "1", "tf": 2},
            {"term": "engine", "doc_id": "1", "tf": 1},
        ],
        "documents": [{
            "doc_id": "1",
            "url": "https://example.com",
            "title": "Example",
            "text": "Search engine example",
            "links": [],
        }],
        "document_stats": [{"doc_id": "1", "doc_len": 3}],
        "term_stats": [
            {"term": "search", "df": 1},
            {"term": "engine", "df": 1},
        ],
        "pagerank": [{"doc_id": "1", "pagerank": 1.0}],
        "corpus_stats": {"N": 1, "avgdl": 3.0},
    }


def test_successful_export(tmp_path, export_data):
    result = export_parquet(version=0, output_dir=tmp_path, **export_data)
    assert result == tmp_path / "serving" / "v0"
    assert (result / "manifest.json").is_file()
    manifest = json.loads((result / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "complete"
    assert manifest["version"] == 0
    assert all(item["valid"] for item in validate_export(result).values())
    assert (result / "statistics" / "document_stats" / "part-00000.parquet").is_file()
    assert (result / "statistics" / "term_stats" / "part-00000.parquet").is_file()


def test_real_parquet_file(tmp_path, export_data):
    result = export_parquet(version=0, output_dir=tmp_path, **export_data)
    table = pq.read_table(result / "postings" / "part-00000.parquet")
    assert table.num_rows == 2


def test_existing_version_not_overwritten(tmp_path, export_data):
    export_parquet(version=0, output_dir=tmp_path, **export_data)
    with pytest.raises(FileExistsError):
        export_parquet(version=0, output_dir=tmp_path, **export_data)


def test_failed_export_not_published(tmp_path, export_data):
    export_data["postings"] = [{"term": "search", "doc_id": "1", "tf": -1}]
    with pytest.raises(ValueError):
        export_parquet(version=1, output_dir=tmp_path, **export_data)
    assert not (tmp_path / "serving" / "v1").exists()
