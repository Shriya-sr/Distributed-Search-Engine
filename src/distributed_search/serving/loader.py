
"""Load and validate a completed local Parquet serving export."""

import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from distributed_search.export.parquet_export import validate_export


DATASET_NAMES = (
    "postings",
    "documents",
    "document_stats",
    "term_stats",
    "pagerank",
)


def load_serving_data(export_dir: str | Path) -> dict[str, Any]:
    """Validate and load all datasets from a completed serving version."""
    export_dir = Path(export_dir)

    if not export_dir.is_dir():
        raise FileNotFoundError(f"Serving export directory not found: {export_dir}")

    manifest_path = export_dir / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError("Export manifest is missing")

    with manifest_path.open("r", encoding="utf-8") as file:
        manifest = json.load(file)

    if manifest.get("status") != "complete":
        raise ValueError("Export is not marked complete")

    corpus_stats = manifest.get("corpus_stats")
    if not isinstance(corpus_stats, dict):
        raise ValueError("Manifest corpus_stats entry is invalid")

    if "N" not in corpus_stats or "avgdl" not in corpus_stats:
        raise ValueError("Manifest corpus_stats must contain N and avgdl")

    # Reuse the project's existing schema, path and row-count validation.
    validate_export(export_dir)

    datasets = manifest["datasets"]
    loaded_data = {}

    for dataset_name in DATASET_NAMES:
        relative_path = Path(datasets[dataset_name]["path"])
        parquet_path = export_dir / relative_path

        table = pq.read_table(parquet_path)
        loaded_data[dataset_name] = table.to_pylist()

    return {
        "version": manifest["version"],
        "corpus_stats": corpus_stats,
        **loaded_data,
    }
