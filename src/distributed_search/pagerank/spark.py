
"""Distributed PageRank implementation using Apache Spark RDDs."""


def compute_pagerank_rdd(
    documents_rdd,
    iterations: int = 10,
    damping_factor: float = 0.85,
):
    """
    Compute PageRank using distributed Spark RDD operations.

    Args:
        documents_rdd: RDD of (doc_id, outgoing_links) pairs.
        iterations: Number of PageRank iterations (minimum 10).
        damping_factor: Probability of following a link.

    Returns:
        RDD of (doc_id, pagerank) pairs.
    """
    if isinstance(iterations, bool) or not isinstance(iterations, int):
        raise ValueError("Iterations must be an integer")

    if iterations < 10:
        raise ValueError("PageRank requires at least 10 iterations")

    if (
        isinstance(damping_factor, bool)
        or not isinstance(damping_factor, (int, float))
        or not 0.0 < damping_factor < 1.0
    ):
        raise ValueError("Damping factor must be between 0 and 1")

    # Extract all document IDs.
    nodes = documents_rdd.map(
        lambda row: row[0]
    ).distinct().cache()

    number_of_nodes = nodes.count()

    if number_of_nodes == 0:
        return nodes.map(lambda doc_id: (doc_id, 0.0))

    valid_nodes = nodes.map(
        lambda doc_id: (doc_id, True)
    ).cache()

    try:
        # Convert outgoing links into (target, source) pairs.
        raw_edges = documents_rdd.flatMap(
            lambda row: (
                (target, row[0])
                for target in set(row[1] or [])
                if target
            )
        ).distinct()

        # Remove edges pointing to nonexistent documents.
        valid_edges = raw_edges.join(valid_nodes).map(
            lambda item: (item[1][0], item[0])
        )

        # Group outgoing destinations by source document.
        outgoing = valid_edges.groupByKey().mapValues(
            lambda targets: tuple(targets)
        )

        # Preserve documents with no outgoing links.
        adjacency = nodes.map(
            lambda doc_id: (doc_id, ())
        ).leftOuterJoin(outgoing).mapValues(
            lambda result: (
                result[1] if result[1] is not None else ()
            )
        ).cache()

        try:
            # Initialize every document with equal rank.
            ranks = nodes.map(
                lambda doc_id: (
                    doc_id,
                    1.0 / number_of_nodes
                )
            ).cache()

            ranks.count()

            try:
                # Perform iterative PageRank.
                for _ in range(iterations):

                    current = adjacency.join(ranks).cache()

                    try:
                        # Calculate rank belonging to dangling nodes.
                        dangling_mass = current.filter(
                            lambda item: len(item[1][0]) == 0
                        ).map(
                            lambda item: item[1][1]
                        ).sum()

                        # Distribute rank across outgoing links.
                        contributions = current.flatMap(
                            lambda item: (
                                (
                                    target,
                                    item[1][1] / len(item[1][0])
                                )
                                for target in item[1][0]
                            )
                        ).reduceByKey(
                            lambda first, second: first + second
                        )

                        # Calculate the next rank for every document.
                        next_ranks = nodes.map(
                            lambda doc_id: (doc_id, 0.0)
                        ).leftOuterJoin(contributions).mapValues(
                            lambda result: (
                                (1.0 - damping_factor)
                                / number_of_nodes
                                + damping_factor * (
                                    (
                                        result[1]
                                        if result[1] is not None
                                        else 0.0
                                    )
                                    + dangling_mass / number_of_nodes
                                )
                            )
                        ).cache()

                        # Materialize before releasing the previous ranks.
                        next_ranks.count()

                    except Exception:
                        current.unpersist()
                        raise

                    current.unpersist()
                    ranks.unpersist()
                    ranks = next_ranks

                # Return the final distributed rank RDD.
                result = ranks
                ranks = None
                return result

            finally:
                if ranks is not None:
                    ranks.unpersist()

        finally:
            adjacency.unpersist()

    finally:
        valid_nodes.unpersist()
        nodes.unpersist()
