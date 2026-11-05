# Professor-provided BM25 reference

`bm25_reference.py` is an **unchanged copy** of the course BM25 example supplied by the user. Treat it as reference code, not as the final serving implementation.

## Input shape caveat

The function docstring documents `docs_df` with `[doc_id, length]` and `index_df` with `[term, doc_id, tf, df]`. But the loop calls `row["length"]` on a row from `index_df`. Running exactly the documented index schema with matching query terms will raise `KeyError: 'length'`. Either enrich the index rows with document length, or obtain each document's length from `docs_df` via `doc_id`. Our agreed contract intentionally stores document length in document statistics; the production Node 4 code should join/lookup it.

This is an interface mismatch, **not** an error in the BM25 formula. The reference does not implement normalized 0.7 BM25 + 0.3 PageRank final scoring; that is a separate step. Keep the professor sample unchanged and implement the working service in `src/distributed_search/search/`.
