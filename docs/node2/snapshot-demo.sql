-- DEMONSTRATION ONLY: Do not rerun blindly on an existing table.
CREATE TABLE IF NOT EXISTS search.db.snapshot_demo (
    doc_id STRING,
    title STRING
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

INSERT INTO search.db.snapshot_demo
VALUES ('doc1', 'First document');

INSERT INTO search.db.snapshot_demo
VALUES ('doc2', 'Second document');

SELECT * FROM search.db.snapshot_demo
ORDER BY doc_id;

SELECT snapshot_id, parent_id, operation, committed_at
FROM search.db.snapshot_demo.snapshots
ORDER BY committed_at;
