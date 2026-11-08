"""Create and validate versioned Parquet exports."""

from __future__ import annotations

import json
import math
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pyarrow as pa
import pyarrow.parquet as pq


SCHEMAS = {
    "postings": pa.schema([
        ("term", pa.string()),
        ("doc_id", pa.string()),
        ("tf", pa.int64()),
    ]),
    "documents": pa.schema([
        ("doc_id", pa.string()),
        ("url", pa.string()),
        ("title", pa.string()),
        ("text", pa.string()),
        ("links", pa.list_(pa.string())),
    ]),
    "document_stats": pa.schema([
        ("doc_id", pa.string()),
        ("doc_len", pa.int64()),
    ]),
    "term_stats": pa.schema([
        ("term", pa.string()),
        ("df", pa.int64()),
    ]),
    "pagerank": pa.schema([
        ("doc_id", pa.string()),
        ("pagerank", pa.float64()),
    ]),
}


def _normalise_records(
    records: Iterable[Mapping[str, Any]], dataset_name: str
) -> list[dict[str, Any]]:
    """Convert records to dictionaries and check required fields."""
    if records is None:
        raise ValueError(f"{dataset_name} cannot be None")

    normalised = []
    expected_fields = set(SCHEMAS[dataset_name].names)

    for record in records:
        if not isinstance(record, Mapping):
            raise ValueError(f"Every record in {dataset_name} must be a mapping")
        missing = expected_fields - set(record.keys())
        if missing:
            raise ValueError(
                f"{dataset_name} record is missing fields: {sorted(missing)}"
            )
        normalised.append({
            field: record[field] for field in SCHEMAS[dataset_name].names
        })

    return normalised


def _validate_records(records: list[dict[str, Any]], dataset_name: str) -> None:
    """Validate records before writing Parquet files."""
    for position, record in enumerate(records):
        for field in SCHEMAS[dataset_name].names:
            if record[field] is None:
                raise ValueError(
                    f"{dataset_name}[{position}].{field} cannot be null"
                )

        if dataset_name == "postings":
            if not isinstance(record["term"], str) or not record["term"]:
                raise ValueError("Posting term cannot be empty")
            if not isinstance(record["doc_id"], str) or not record["doc_id"]:
                raise ValueError("Posting doc_id cannot be empty")
            tf = record["tf"]
            if isinstance(tf, bool) or not isinstance(tf, int) or tf <= 0:
                raise ValueError("Posting tf must be a positive integer")

        elif dataset_name == "documents":
            if not isinstance(record["doc_id"], str) or not record["doc_id"]:
                raise ValueError("Document doc_id cannot be empty")
            for field in ("url", "title", "text"):
                if not isinstance(record[field], str):
                    raise ValueError(f"Document {field} must be a string")
            if not isinstance(record["links"], list):
                raise ValueError("Document links must be a list")
            if not all(isinstance(link, str) for link in record["links"]):
                raise ValueError("Every document link must be a string")

        elif dataset_name == "document_stats":
            if not isinstance(record["doc_id"], str) or not record["doc_id"]:
                raise ValueError("Document stats doc_id cannot be empty")
            doc_len = record["doc_len"]
            if (isinstance(doc_len, bool) or not isinstance(doc_len, int)
                    or doc_len < 0):
                raise ValueError("doc_len must be a nonnegative integer")

        elif dataset_name == "term_stats":
            if not isinstance(record["term"], str) or not record["term"]:
                raise ValueError("Term stats term cannot be empty")
            df = record["df"]
            if isinstance(df, bool) or not isinstance(df, int) or df < 0:
                raise ValueError("df must be a nonnegative integer")

        elif dataset_name == "pagerank":
            if not isinstance(record["doc_id"], str) or not record["doc_id"]:
                raise ValueError("PageRank doc_id cannot be empty")
            rank = record["pagerank"]
            if (isinstance(rank, bool) or not isinstance(rank, (int, float))
                    or not math.isfinite(rank)):
                raise ValueError("PageRank must be a finite number")


def _dataset_path(output_dir: Path, dataset_name: str) -> Path:
    """Return the contract-defined location for a dataset."""
    if dataset_name in {"document_stats", "term_stats"}:
        return output_dir / "statistics" / dataset_name
    return output_dir / dataset_name


def _write_dataset(
    records: list[dict[str, Any]], dataset_name: str, output_dir: Path
) -> dict[str, Any]:
    """Write one dataset as a Parquet part file."""
    dataset_dir = _dataset_path(output_dir, dataset_name)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    schema = SCHEMAS[dataset_name]
    table = pa.Table.from_pylist(records, schema=schema)
    file_path = dataset_dir / "part-00000.parquet"
    pq.write_table(table, file_path, compression="zstd")

    return {
        "path": str(file_path.relative_to(output_dir)),
        "row_count": table.num_rows,
        "schema": [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable,
            }
            for field in schema
        ],
    }


def validate_export(
    export_dir: str | Path, *, allow_validating: bool = False
) -> dict[str, Any]:
    """Validate all Parquet datasets against the export manifest."""
    export_dir = Path(export_dir)
    manifest_path = export_dir / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError("Export manifest is missing")

    with manifest_path.open("r", encoding="utf-8") as file:
        manifest = json.load(file)

    allowed_statuses = {"complete"}
    if allow_validating:
        allowed_statuses.add("validating")
    if manifest.get("status") not in allowed_statuses:
        raise ValueError("Export is not marked complete")

    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict):
        raise ValueError("Manifest datasets entry is invalid")

    results = {}
    for dataset_name, expected_schema in SCHEMAS.items():
        if dataset_name not in datasets:
            raise ValueError(f"Manifest is missing {dataset_name}")
        dataset_info = datasets[dataset_name]
        if not isinstance(dataset_info, dict) or "path" not in dataset_info:
            raise ValueError(f"Invalid manifest entry for {dataset_name}")

        relative_path = Path(dataset_info["path"])
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ValueError(f"Invalid dataset path: {relative_path}")
        file_path = export_dir / relative_path
        if not file_path.is_file():
            raise ValueError(f"Missing Parquet file: {file_path}")

        parquet_file = pq.ParquetFile(file_path)
        actual_schema = parquet_file.schema_arrow
        if not actual_schema.equals(expected_schema, check_metadata=False):
            raise ValueError(
                f"Schema mismatch in {dataset_name}: expected {expected_schema}, "
                f"got {actual_schema}"
            )

        actual_rows = parquet_file.metadata.num_rows
        if actual_rows != dataset_info.get("row_count"):
            raise ValueError(
                f"Row count mismatch in {dataset_name}: "
                f"expected {dataset_info.get('row_count')}, got {actual_rows}"
            )
        results[dataset_name] = {"valid": True, "row_count": actual_rows}

    return results


def export_parquet(
    *,
    version: int,
    output_dir: str | Path,
    postings: Iterable[Mapping[str, Any]],
    documents: Iterable[Mapping[str, Any]],
    document_stats: Iterable[Mapping[str, Any]],
    term_stats: Iterable[Mapping[str, Any]],
    pagerank: Iterable[Mapping[str, Any]],
    corpus_stats: Mapping[str, Any],
) -> Path:
    """Build, validate, then publish a complete serving/vN export."""
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        raise ValueError("version must be a nonnegative integer")
    if not isinstance(corpus_stats, Mapping):
        raise ValueError("corpus_stats must be a mapping")
    if "N" not in corpus_stats or "avgdl" not in corpus_stats:
        raise ValueError("corpus_stats must contain N and avgdl")

    corpus_n = corpus_stats["N"]
    avgdl = corpus_stats["avgdl"]
    if isinstance(corpus_n, bool) or not isinstance(corpus_n, int) or corpus_n < 0:
        raise ValueError("corpus_stats.N must be a nonnegative integer")
    if (isinstance(avgdl, bool) or not isinstance(avgdl, (int, float))
            or not math.isfinite(avgdl) or avgdl < 0):
        raise ValueError("corpus_stats.avgdl must be finite and nonnegative")

    output_dir = Path(output_dir)
    serving_dir = output_dir / "serving"
    serving_dir.mkdir(parents=True, exist_ok=True)
    final_dir = serving_dir / f"v{version}"
    if final_dir.exists():
        raise FileExistsError(f"Export version already exists: {final_dir}")

    staging_dir = serving_dir / f".v{version}.tmp-{uuid.uuid4().hex}"
    staging_dir.mkdir(parents=True)
    datasets = {
        "postings": postings,
        "documents": documents,
        "document_stats": document_stats,
        "term_stats": term_stats,
        "pagerank": pagerank,
    }

    try:
        manifest_datasets = {}
        for dataset_name, records in datasets.items():
            normalised = _normalise_records(records, dataset_name)
            _validate_records(normalised, dataset_name)
            manifest_datasets[dataset_name] = _write_dataset(
                normalised, dataset_name, staging_dir
            )

        manifest = {
            "version": version,
            "status": "validating",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "corpus_stats": {"N": corpus_n, "avgdl": avgdl},
            "datasets": manifest_datasets,
        }
        manifest_path = staging_dir / "manifest.json"
        with manifest_path.open("w", encoding="utf-8") as file:
            json.dump(manifest, file, indent=2)

        validate_export(staging_dir, allow_validating=True)
        manifest["status"] = "complete"
        with manifest_path.open("w", encoding="utf-8") as file:
            json.dump(manifest, file, indent=2)

        os.replace(staging_dir, final_dir)
        return final_dir
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise
