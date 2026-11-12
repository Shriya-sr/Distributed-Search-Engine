"""Distributed Spark inverted index using Iceberg as the source."""
from __future__ import annotations

import argparse
import os
import uuid
from collections import Counter

from distributed_search.indexing.tokenizer import tokenize


def _postings_for_document(row):
    doc_id, content = row
    return [
        (term, str(doc_id), int(tf))
        for term, tf in Counter(tokenize(content or "")).items()
    ]


def _document_length(row):
    doc_id, content = row
    return str(doc_id), len(tokenize(content or ""))


def build_distributed_index(spark, table: str, output: str, snapshot_id=None) -> dict:
    """Compute distributed BM25 statistics and save Parquet to HDFS."""
    if snapshot_id is None:
        raise ValueError("An explicit Iceberg snapshot ID is required")
    from pyspark.sql.types import (
        DoubleType, IntegerType, StringType, StructField, StructType
    )

    reader = spark.read
    if snapshot_id is not None:
        reader = reader.option("snapshot-id", str(snapshot_id))
    pages = reader.format("iceberg").load(table).select("doc_id", "text")
    docs = pages.rdd.map(
        lambda r: (r["doc_id"], r["text"])
    ).cache()

    staging = f"{output}.staging-{uuid.uuid4().hex}"
    try:
        postings_rdd = docs.flatMap(_postings_for_document)
        doc_stats_rdd = docs.map(_document_length)

        postings_schema = StructType([
            StructField("term", StringType(), False),
            StructField("doc_id", StringType(), False),
            StructField("tf", IntegerType(), False),
        ])
        doc_schema = StructType([
            StructField("doc_id", StringType(), False),
            StructField("doc_len", IntegerType(), False),
        ])
        term_schema = StructType([
            StructField("term", StringType(), False),
            StructField("df", IntegerType(), False),
        ])
        corpus_schema = StructType([
            StructField("N", IntegerType(), False),
            StructField("avgdl", DoubleType(), False),
        ])

        term_stats_rdd = postings_rdd.map(
            lambda x: (x[0], 1)
        ).reduceByKey(lambda a, b: a + b)

        n, total_length = doc_stats_rdd.map(
            lambda x: (1, x[1])
        ).fold(
            (0, 0),
            lambda a, b: (a[0] + b[0], a[1] + b[1])
        )

        avgdl = total_length / n if n else 0.0

        spark.createDataFrame(
            postings_rdd, postings_schema
        ).write.mode("errorifexists").parquet(
            f"{staging}/postings"
        )

        spark.createDataFrame(
            doc_stats_rdd, doc_schema
        ).write.mode("errorifexists").parquet(
            f"{staging}/doc_stats"
        )

        spark.createDataFrame(
            term_stats_rdd, term_schema
        ).write.mode("errorifexists").parquet(
            f"{staging}/term_stats"
        )

        spark.createDataFrame(
            [(n, float(avgdl))], corpus_schema
        ).write.mode("errorifexists").parquet(
            f"{staging}/corpus_stats"
        )

        metadata_schema = StructType([
            StructField("snapshot_id", StringType(), False),
        ])
        spark.createDataFrame(
            [(str(snapshot_id),)], metadata_schema
        ).write.mode("errorifexists").parquet(
            f"{staging}/index_metadata"
        )

        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        fs_path = jvm.org.apache.hadoop.fs.Path
        destination = fs_path(output)
        temporary = fs_path(staging)
        fs = destination.getFileSystem(conf)

        if fs.exists(destination):
            raise FileExistsError(
                f"Index already exists: {output}"
            )

        if not fs.rename(temporary, destination):
            raise RuntimeError(
                f"Could not publish index: {output}"
            )

        metadata = {
            "N": n,
            "avgdl": avgdl,
            "output": output,
            "snapshot_id": snapshot_id,
        }
        return metadata
    finally:
        docs.unpersist()


def main():
    parser = argparse.ArgumentParser(
        description="Build distributed Iceberg inverted index"
    )
    parser.add_argument(
        "--version", required=True, choices=["v0", "v1"]
    )
    parser.add_argument(
        "--table", default="search.db.pages"
    )
    parser.add_argument(
        "--snapshot-id", type=int, required=True
    )
    parser.add_argument(
        "--output-root",
        default="hdfs://node2:9000/distributed-search/index"
    )
    args = parser.parse_args()

    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(
        f"distributed-index-{args.version}"
    )

    master = os.environ.get("SPARK_MASTER_URL")
    if master:
        builder = builder.master(master)

    warehouse = os.environ.get(
        "ICEBERG_WAREHOUSE",
        "hdfs://node2:9000/distributed-search/warehouse"
    )

    builder = (
        builder.config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.sql.catalog.search",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.search.type", "hadoop")
        .config("spark.sql.catalog.search.warehouse", warehouse)
    )

    spark = builder.getOrCreate()

    try:
        output = f"{args.output_root.rstrip('/')}/{args.version}"
        result = build_distributed_index(
            spark, args.table, output, args.snapshot_id
        )
        print(result)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
