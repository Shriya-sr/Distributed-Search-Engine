"""Bridge Iceberg, HDFS index, Spark PageRank and local serving export."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from distributed_search.export.index_adapter import (
    normalize_records,
    validate_index_statistics,
)
from distributed_search.export.parquet_export import export_parquet
from distributed_search.pagerank.spark import compute_pagerank_rdd


def _collect_bounded(frame, name, max_rows):
    """Prevent accidentally collecting an unbounded Spark dataset."""
    rows = frame.limit(max_rows + 1).collect()
    if len(rows) > max_rows:
        raise ValueError(
            f"{name} exceeds {max_rows} rows; "
            "the local serving exporter cannot safely handle it"
        )
    return [row.asDict(recursive=True) for row in rows]


def export_pipeline(
    spark,
    *,
    version,
    snapshot_id,
    table="search.db.pages",
    index_root="hdfs://node2:9000/distributed-search/index",
    output_root="data",
    max_rows=100000,
):
    """Build a serving export from a specific Iceberg snapshot."""
    if version not in (0, 1):
        raise ValueError("version must be 0 or 1")
    if snapshot_id is None:
        raise ValueError("An explicit Iceberg snapshot ID is required")
    if max_rows <= 0:
        raise ValueError("max_rows must be positive")

    index_path = f"{index_root.rstrip('/')}/v{version}"

    metadata_rows = _collect_bounded(
        spark.read.parquet(f"{index_path}/index_metadata"),
        "index_metadata",
        1,
    )
    if len(metadata_rows) != 1:
        raise ValueError("Expected exactly one index_metadata record")

    indexed_snapshot = metadata_rows[0]["snapshot_id"]
    if indexed_snapshot != str(snapshot_id):
        raise ValueError(
            f"Snapshot mismatch: index={indexed_snapshot}, "
            f"requested={snapshot_id}"
        )

    reader = spark.read.option("snapshot-id", str(snapshot_id))
    pages = reader.format("iceberg").load(table)

    required = {"doc_id", "url", "title", "text", "links"}
    missing = required - set(pages.columns)
    if missing:
        raise ValueError(f"Iceberg table missing columns: {sorted(missing)}")

    documents = _collect_bounded(
        pages.select("doc_id", "url", "title", "text", "links"),
        "documents",
        max_rows,
    )

    documents = [
        {
            "doc_id": str(doc["doc_id"]),
            "url": doc["url"],
            "title": doc["title"],
            "text": doc["text"],
            "links": doc["links"],
        }
        for doc in documents
    ]

    postings = normalize_records(
        "postings",
        _collect_bounded(
            spark.read.parquet(f"{index_path}/postings"),
            "postings",
            max_rows,
        ),
    )
    document_stats = normalize_records(
        "doc_stats",
        _collect_bounded(
            spark.read.parquet(f"{index_path}/doc_stats"),
            "doc_stats",
            max_rows,
        ),
    )
    term_stats = normalize_records(
        "term_stats",
        _collect_bounded(
            spark.read.parquet(f"{index_path}/term_stats"),
            "term_stats",
            max_rows,
        ),
    )

    stats_rows = _collect_bounded(
        spark.read.parquet(f"{index_path}/corpus_stats"),
        "corpus_stats",
        1,
    )
    if len(stats_rows) != 1:
        raise ValueError("Expected exactly one corpus_stats record")

    corpus_stats = validate_index_statistics(stats_rows[0])

    if corpus_stats["N"] != len(documents):
        raise ValueError(
            "Index document count does not match Iceberg snapshot. "
            "Rebuild the index using the requested snapshot ID."
        )

    # Resolve outgoing URLs to document IDs using distributed Spark joins.
    # No full graph collection on the driver.
    from pyspark.sql import functions as F

    graph_pages = pages.select(
        F.col("doc_id").cast("string").alias("doc_id"),
        F.col("url").alias("url"),
        F.col("links").alias("links"),
    )

    url_lookup = graph_pages.select(
        F.col("url").alias("target_url"),
        F.col("doc_id").alias("target_id"),
    )

    edges = (
        graph_pages
        .select(
            "doc_id",
            F.explode_outer("links").alias("target_url"),
        )
        .join(url_lookup, on="target_url", how="inner")
        .select("doc_id", "target_id")
        .distinct()
    )

    graph_rdd = (
        graph_pages.select("doc_id").distinct()
        .join(
            edges.groupBy("doc_id").agg(
                F.collect_set("target_id").alias("targets")
            ),
            on="doc_id",
            how="left",
        )
        .select("doc_id", "targets")
        .rdd.map(
            lambda row: (
                row["doc_id"],
                row["targets"] or [],
            )
        )
    )

    pagerank_rdd = compute_pagerank_rdd(
        graph_rdd,
        iterations=10,
        damping_factor=0.85,
    )
    try:
        pagerank = [
            {"doc_id": str(doc_id), "pagerank": float(score)}
            for doc_id, score in pagerank_rdd.collect()
        ]
    finally:
        pagerank_rdd.unpersist()

    result = export_parquet(
        version=version,
        output_dir=output_root,
        postings=postings,
        documents=documents,
        document_stats=document_stats,
        term_stats=term_stats,
        pagerank=pagerank,
        corpus_stats=corpus_stats,
    )

    return str(result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", type=int, choices=[0, 1], required=True)
    parser.add_argument("--snapshot-id", type=int, required=True)
    parser.add_argument("--table", default="search.db.pages")
    parser.add_argument(
        "--index-root",
        default="hdfs://node2:9000/distributed-search/index",
    )
    parser.add_argument("--output-root", default="data")
    parser.add_argument("--max-rows", type=int, default=100000)
    args = parser.parse_args()

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(f"search-export-v{args.version}")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.search",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.search.type", "hadoop")
        .config(
            "spark.sql.catalog.search.warehouse",
            "hdfs://node2:9000/distributed-search/warehouse",
        )
        .getOrCreate()
    )

    try:
        result = export_pipeline(
            spark,
            version=args.version,
            snapshot_id=args.snapshot_id,
            table=args.table,
            index_root=args.index_root,
            output_root=args.output_root,
            max_rows=args.max_rows,
        )
        print(json.dumps({"export": result}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
