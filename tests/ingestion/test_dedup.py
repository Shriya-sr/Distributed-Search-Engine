"""tests/ingestion/test_dedup.py"""
from distributed_search.ingestion import deduplicate_pages

_BASE = {"url": "u", "title": "t", "text": "x", "links": []}


def _doc(doc_id: str) -> dict:
    return {"doc_id": doc_id, **_BASE}


def test_no_duplicates_unchanged():
    recs = [_doc("1"), _doc("2"), _doc("3")]
    result = deduplicate_pages(recs)
    assert len(result) == 3
    assert set(result.keys()) == {"1", "2", "3"}


def test_duplicate_ids_collapsed():
    recs = [_doc("1"), _doc("2"), _doc("1")]  # doc "1" appears twice
    result = deduplicate_pages(recs)
    assert len(result) == 2


def test_returns_dict_keyed_by_doc_id():
    recs = [_doc("42")]
    result = deduplicate_pages(recs)
    assert "42" in result
    assert result["42"]["doc_id"] == "42"


def test_phase0_contract_1000_unique():
    """
    Simulate the contract: 1020 valid records with 20 duplicated IDs
    (all copies identical) should produce 1000 unique entries.
    """
    unique_ids = [str(i) for i in range(1, 1001)]
    # 980 unique + 20 that appear twice = 1020 records
    extra = [_doc(str(i)) for i in range(1, 21)]
    recs = [_doc(uid) for uid in unique_ids] + extra
    assert len(recs) == 1020
    result = deduplicate_pages(recs)
    assert len(result) == 1000
