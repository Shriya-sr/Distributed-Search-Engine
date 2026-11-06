"""tests/ingestion/test_link_cleaner.py"""
from distributed_search.ingestion import clean_dangling_links


def _doc(doc_id: str, links: list[str]) -> dict:
    return {"doc_id": doc_id, "url": "u", "title": "t", "text": "x", "links": links}


def test_good_links_preserved():
    live = {
        "1": _doc("1", ["2"]),
        "2": _doc("2", []),
    }
    cleaned = clean_dangling_links(live)
    assert cleaned["1"]["links"] == ["2"]


def test_dangling_link_removed():
    live = {
        "1": _doc("1", ["2", "99"]),  # 99 doesn't exist
        "2": _doc("2", []),
    }
    cleaned = clean_dangling_links(live)
    assert cleaned["1"]["links"] == ["2"]
    assert "99" not in cleaned["1"]["links"]


def test_all_links_dangling():
    live = {
        "1": _doc("1", ["99", "100"]),
    }
    cleaned = clean_dangling_links(live)
    assert cleaned["1"]["links"] == []


def test_no_links_field_treated_as_empty():
    live = {
        "1": {"doc_id": "1", "url": "u", "title": "t", "text": "x"},
    }
    cleaned = clean_dangling_links(live)
    assert cleaned["1"]["links"] == []


def test_original_dict_not_mutated():
    live = {
        "1": _doc("1", ["99"]),
    }
    _ = clean_dangling_links(live)
    # Original should still have the dangling link
    assert live["1"]["links"] == ["99"]


def test_phase1_contract_link_count():
    """
    After Phase 1 cleanup, 467 links survive (down from 474).
    Simulate: 7 links removed.
    """
    # Build a live set where doc "A" has 7 links to deleted IDs + others
    live: dict[str, dict] = {}
    # 467 valid target docs
    for i in range(1, 468):
        live[str(i)] = _doc(str(i), [])
    # Source doc with 467 valid + 7 dangling links
    all_links = [str(i) for i in range(1, 468)] + ["9001", "9002", "9003", "9004", "9005", "9006", "9007"]
    live["source"] = _doc("source", all_links)

    cleaned = clean_dangling_links(live)
    assert len(cleaned["source"]["links"]) == 467
