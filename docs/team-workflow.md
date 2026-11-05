# Team collaboration workflow

1. Pick an issue from `planning/issues.md` and create a branch (`feature/node2-hdfs`, etc.).
2. Keep PRs focused on your module. Avoid changing `common/`, `contracts.md`, or `pyproject.toml` without informing the whole team.
3. Include: what changed, how tested, whether this affects another VM, and screenshots/log snippets for cluster changes.
4. Run `python -m pytest -q` locally before requesting review.
5. Only merge through PR review; don't push straight to main. Keep actual credentials, access tokens, private IP mappings and datasets out of Git.
6. Develop with fake/toy fixtures; cluster integration happens at agreed sessions.
7. Assign one integration owner for each boundary: Node1→Node2, Node2↔Node3, Node3→Node4.

A PR touching shared interfaces should receive a review from all affected components. Keep `main` runnable and documented.
