
"""Reference PageRank implementation for testing."""

from collections.abc import Mapping, Sequence


def compute_pagerank(
    graph: Mapping[str, Sequence[str]],
    iterations: int = 10,
    damping_factor: float = 0.85,
) -> dict[str, float]:
    """
    Compute PageRank with dangling-node redistribution.

    Args:
        graph: Mapping of document IDs to outgoing document IDs.
        iterations: Number of PageRank iterations (minimum 10).
        damping_factor: Probability of following a link.

    Returns:
        Mapping of document IDs to their PageRank scores.
    """
    if iterations < 10:
        raise ValueError("PageRank requires at least 10 iterations")

    if not 0.0 < damping_factor < 1.0:
        raise ValueError("Damping factor must be between 0 and 1")

    if not graph:
        return {}

    if any(not isinstance(doc_id, str) or not doc_id for doc_id in graph):
        raise ValueError("Document IDs must be non-empty strings")

    nodes = set(graph)

    # Remove missing targets and duplicate outgoing links.
    links = {}

    for doc_id, destinations in graph.items():
        if isinstance(destinations, str):
            raise ValueError("Outgoing links must be a sequence of IDs")

        links[doc_id] = tuple(
            sorted({
                target
                for target in destinations
                if target in nodes
            })
        )

    n = len(nodes)

    # Initialize all documents with equal rank.
    ranks = {
        doc_id: 1.0 / n
        for doc_id in nodes
    }

    # Perform iterative PageRank.
    for _ in range(iterations):

        # Total rank belonging to dangling nodes.
        dangling_mass = sum(
            ranks[doc_id]
            for doc_id in nodes
            if not links[doc_id]
        )

        new_ranks = {}

        for doc_id in nodes:

            incoming_rank = 0.0

            for source in nodes:
                destinations = links[source]

                if doc_id in destinations:
                    incoming_rank += (
                        ranks[source] / len(destinations)
                    )

            new_ranks[doc_id] = (
                (1.0 - damping_factor) / n
                + damping_factor * (
                    incoming_rank + dangling_mass / n
                )
            )

        ranks = new_ranks

    return ranks
