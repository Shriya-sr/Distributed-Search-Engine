-- DEMONSTRATION ONLY: Snapshot IDs are specific to the original test run.
-- Read the current table (two documents)
SELECT * FROM search.db.snapshot_demo
ORDER BY doc_id;

-- Time travel to the first snapshot (one document)
SELECT * FROM search.db.snapshot_demo
VERSION AS OF 1853666877471679633
ORDER BY doc_id;

-- Restore the demo table to its first snapshot
CALL search.system.rollback_to_snapshot(
    'db.snapshot_demo',
    1853666877471679633
);

-- Verify the restored contents
SELECT * FROM search.db.snapshot_demo
ORDER BY doc_id;

-- Inspect snapshot history
SELECT snapshot_id, parent_id, operation
FROM search.db.snapshot_demo.snapshots;
