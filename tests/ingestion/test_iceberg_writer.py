"""
tests/ingestion/test_iceberg_writer.py

Tests for the PySpark + Iceberg write logic in scripts/node1/run_ingestion.py.

All Spark interactions are mocked so these tests run in CI without a real
cluster or PySpark installed.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Make the script importable without PySpark being installed.
# We stub out pyspark before importing the script module.
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parents[2]

# Build a minimal pyspark stub so the deferred imports inside the script work.
def _make_pyspark_stub():
    pyspark = ModuleType("pyspark")
    sql_mod = ModuleType("pyspark.sql")
    types_mod = ModuleType("pyspark.sql.types")

    # Stub types used by _doc_schema()
    for name in ("StructType", "StructField", "StringType", "ArrayType"):
        setattr(types_mod, name, MagicMock(name=name))

    pyspark.sql = sql_mod
    sql_mod.types = types_mod
    sql_mod.SparkSession = MagicMock(name="SparkSession")

    sys.modules.setdefault("pyspark", pyspark)
    sys.modules.setdefault("pyspark.sql", sql_mod)
    sys.modules.setdefault("pyspark.sql.types", types_mod)


_make_pyspark_stub()

# Now import the script module
sys.path.insert(0, str(_REPO_ROOT / "scripts" / "node1"))
import run_ingestion as ri  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_spark(rows=None):
    """Return a mock SparkSession whose .sql() and .collect() behave sensibly."""
    spark = MagicMock(name="SparkSession")
    df = MagicMock(name="DataFrame")
    df.collect.return_value = rows or []
    spark.sql.return_value = df
    spark.createDataFrame.return_value = df
    return spark


def _doc(doc_id: str, links=None) -> dict:
    return {
        "doc_id": doc_id,
        "url": f"https://example.org/{doc_id}",
        "title": f"Title {doc_id}",
        "text": f"text for doc {doc_id}",
        "links": links or [],
    }


# ---------------------------------------------------------------------------
# _merge_into_iceberg
# ---------------------------------------------------------------------------

def test_merge_calls_spark_sql():
    spark = _make_spark()
    live_docs = {"1": _doc("1"), "2": _doc("2")}
    ri._merge_into_iceberg(live_docs, spark)
    # Should create a temp view and then call MERGE INTO
    assert spark.createDataFrame.called
    sql_calls = [str(c) for c in spark.sql.call_args_list]
    assert any("MERGE INTO" in s for s in sql_calls)


def test_merge_uses_correct_table():
    spark = _make_spark()
    ri._merge_into_iceberg({"1": _doc("1")}, spark)
    merge_call = next(
        (str(c) for c in spark.sql.call_args_list if "MERGE" in str(c)), ""
    )
    assert "search.db.pages" in merge_call


def test_merge_includes_all_fields():
    spark = _make_spark()
    ri._merge_into_iceberg({"1": _doc("1")}, spark)
    merge_sql = next(
        (str(c) for c in spark.sql.call_args_list if "MERGE" in str(c)), ""
    )
    for field in ("doc_id", "url", "title", "text", "links"):
        assert field in merge_sql


# ---------------------------------------------------------------------------
# _delete_from_iceberg
# ---------------------------------------------------------------------------

def test_delete_noop_on_empty_set():
    spark = _make_spark()
    ri._delete_from_iceberg(set(), spark)
    spark.sql.assert_not_called()


def test_delete_calls_delete_statement():
    spark = _make_spark()
    ri._delete_from_iceberg({"42", "99"}, spark)
    sql_text = str(spark.sql.call_args_list[0])
    assert "DELETE FROM" in sql_text
    assert "search.db.pages" in sql_text
    assert "42" in sql_text


def test_delete_quotes_ids():
    spark = _make_spark()
    ri._delete_from_iceberg({"7"}, spark)
    sql_text = str(spark.sql.call_args_list[0])
    assert "'7'" in sql_text


# ---------------------------------------------------------------------------
# _read_iceberg
# ---------------------------------------------------------------------------

class _FakeRow:
    def __init__(self, doc_id, url, title, text, links):
        self._d = {
            "doc_id": doc_id,
            "url": url,
            "title": title,
            "text": text,
            "links": links,
        }

    def __getitem__(self, key):
        return self._d[key]


def test_read_iceberg_returns_dict():
    rows = [_FakeRow("1", "u", "t", "x", ["2"])]
    spark = _make_spark(rows=rows)
    result = ri._read_iceberg(spark)
    assert "1" in result
    assert result["1"]["doc_id"] == "1"
    assert result["1"]["links"] == ["2"]


def test_read_iceberg_handles_null_links():
    rows = [_FakeRow("5", "u", "t", "x", None)]
    spark = _make_spark(rows=rows)
    result = ri._read_iceberg(spark)
    assert result["5"]["links"] == []


def test_read_iceberg_raises_on_spark_error():
    spark = MagicMock()
    spark.sql.side_effect = Exception("Table not found")
    with pytest.raises(RuntimeError, match="Phase 0 has been ingested"):
        ri._read_iceberg(spark)


# ---------------------------------------------------------------------------
# run_phase0 (with mock Spark)
# ---------------------------------------------------------------------------

def test_run_phase0_calls_merge(tmp_path):
    # Write a minimal valid pages.jsonl fixture
    pages = tmp_path / "pages.jsonl"
    pages.write_text(
        '{"doc_id":"1","url":"u","title":"t","text":"hello world","links":[]}\n'
        '{"doc_id":"2","url":"u","title":"t","text":"foo bar","links":["1"]}\n'
    )
    spark = _make_spark()
    ri.run_phase0(str(pages), spark=spark)
    # MERGE must have been called
    merge_calls = [c for c in spark.sql.call_args_list if "MERGE" in str(c)]
    assert len(merge_calls) >= 1


def test_run_phase0_dry_run_no_spark_calls(tmp_path):
    pages = tmp_path / "pages.jsonl"
    pages.write_text(
        '{"doc_id":"1","url":"u","title":"t","text":"x"}\n'
    )
    # Pass spark=None → dry-run mode; no Spark calls should happen
    ri.run_phase0(str(pages), spark=None)  # should not raise


# ---------------------------------------------------------------------------
# run_phase1 (with mock Spark)
# ---------------------------------------------------------------------------

def test_run_phase1_deletes_removed_docs(tmp_path):
    changes = tmp_path / "changes.jsonl"
    changes.write_text('{"doc_id":"1","op":"DELETE"}\n')

    # Simulate Iceberg having doc "1" and "2"
    rows = [
        _FakeRow("1", "u", "t", "x", []),
        _FakeRow("2", "u", "t", "y", []),
    ]
    spark = _make_spark(rows=rows)

    ri.run_phase1(str(changes), spark=spark)

    delete_calls = [c for c in spark.sql.call_args_list if "DELETE FROM" in str(c)]
    assert len(delete_calls) == 1
    assert "'1'" in str(delete_calls[0])


def test_run_phase1_upserts_new_doc(tmp_path):
    changes = tmp_path / "changes.jsonl"
    changes.write_text(
        '{"doc_id":"99","op":"UPSERT","url":"u","title":"t","text":"new doc","links":[]}\n'
    )
    rows = [_FakeRow("1", "u", "t", "x", [])]
    spark = _make_spark(rows=rows)

    ri.run_phase1(str(changes), spark=spark)

    merge_calls = [c for c in spark.sql.call_args_list if "MERGE" in str(c)]
    assert len(merge_calls) >= 1


def test_run_phase1_idempotent(tmp_path):
    """Applying the same changes twice should not raise or produce extra Iceberg calls."""
    changes = tmp_path / "changes.jsonl"
    changes.write_text('{"doc_id":"1","op":"DELETE"}\n')

    rows = [_FakeRow("1", "u", "t", "x", []), _FakeRow("2", "u", "t", "y", [])]

    # First run
    spark1 = _make_spark(rows=rows)
    ri.run_phase1(str(changes), spark=spark1)

    # Second run: Iceberg now reflects post-Phase-1 state (doc "1" is gone)
    rows_after = [_FakeRow("2", "u", "t", "y", [])]
    spark2 = _make_spark(rows=rows_after)
    ri.run_phase1(str(changes), spark=spark2)

    # Both runs should complete without error; delete should be no-op on second run
    delete_calls_run2 = [
        c for c in spark2.sql.call_args_list if "DELETE FROM" in str(c)
    ]
    # doc "1" is already gone → no DELETE needed in second run
    assert len(delete_calls_run2) == 0


def test_run_phase1_dry_run_no_spark(tmp_path):
    changes = tmp_path / "changes.jsonl"
    changes.write_text('{"doc_id":"1","op":"DELETE"}\n')
    pages = tmp_path / "pages.jsonl"
    pages.write_text(
        '{"doc_id":"1","url":"u","title":"t","text":"x"}\n'
        '{"doc_id":"2","url":"u","title":"t","text":"y"}\n'
    )
    # spark=None → dry-run; must not raise
    ri.run_phase1(str(changes), spark=None, pages_path=str(pages))


# ---------------------------------------------------------------------------
# Defaults and rerun idempotency tests
# ---------------------------------------------------------------------------

def test_default_constants():
    assert "1.8.1" in ri._DEFAULT_JAR
    assert ri._DEFAULT_WAREHOUSE == "hdfs://node2:9000/distributed-search/warehouse"


def test_get_spark_uses_default_warehouse_and_jar(monkeypatch):
    monkeypatch.setenv("SPARK_MASTER_URL", "spark://172.25.240.223:7077")
    monkeypatch.delenv("ICEBERG_WAREHOUSE", raising=False)
    monkeypatch.delenv("ICEBERG_JAR_PACKAGE", raising=False)

    mock_builder = MagicMock()
    mock_session = MagicMock()
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_session

    with patch("pyspark.sql.SparkSession.builder", mock_builder):
        spark = ri._get_spark()
        config_calls = mock_builder.config.call_args_list
        assert any(
            c == call("spark.sql.catalog.search.warehouse", "hdfs://node2:9000/distributed-search/warehouse")
            for c in config_calls
        )
        assert any(
            c == call("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.8.1")
            for c in config_calls
        )


def test_run_phase0_rerun_idempotent(tmp_path):
    """Running Phase 0 multiple times with identical docs must use MERGE to prevent duplicates."""
    pages = tmp_path / "pages.jsonl"
    pages.write_text(
        '{"doc_id":"1","url":"u","title":"t","text":"x","links":[]}\n'
        '{"doc_id":"2","url":"u","title":"t","text":"y","links":["1"]}\n'
    )
    spark = _make_spark()

    # Run 1
    ri.run_phase0(str(pages), spark=spark)
    # Run 2 (rerun)
    ri.run_phase0(str(pages), spark=spark)

    merge_calls = [c for c in spark.sql.call_args_list if "MERGE INTO search.db.pages" in str(c)]
    assert len(merge_calls) == 2
    # Verify MERGE clause uses doc_id matching to prevent duplicates
    assert "t.doc_id = s.doc_id" in str(merge_calls[0])
    assert "WHEN MATCHED THEN" in str(merge_calls[0])
    assert "WHEN NOT MATCHED THEN" in str(merge_calls[0])

