CREATE NAMESPACE IF NOT EXISTS search.db;

CREATE TABLE IF NOT EXISTS search.db.pages (
    doc_id STRING,
    url STRING,
    title STRING,
    text STRING,
    links ARRAY<STRING>
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2'
);

SHOW TABLES IN search.db;
