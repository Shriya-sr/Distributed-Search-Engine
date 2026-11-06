"""tests/ingestion/test_apply_changes.py"""
from pathlib import Path
from distributed_search.ingestion import (
    load_pages_jsonl,
    load_changes_jsonl,
    validate_page,
    deduplicate_pages,
    apply_phase1,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _load_toy_phase0() -> dict[str, dict]:
    records, _ = load_pages_jsonl(FIXTURES / "toy_pages.jsonl")
    valid = [r for r in records if validate_page(r)[0]]
    return deduplicate_pages(valid)


def test_toy_delete_removes_docs():
    live = _load_toy_phase0()
    assert "2" in live
    assert "3" in live
    changes, _ = load_changes_jsonl(FIXTURES / "toy_changes.jsonl")
    new_live, rejected = apply_phase1(live, changes)
    assert "2" not in new_live
    assert "3" not in new_live
    assert len(rejected) == 0


def test_toy_upsert_replaces_existing():
    live = _load_toy_phase0()
    changes, _ = load_changes_jsonl(FIXTURES / "toy_changes.jsonl")
    new_live, _ = apply_phase1(live, changes)
    # doc "1" was upserted with new title
    assert new_live["1"]["title"] == "Alpha Updated"


def test_toy_upsert_inserts_new_doc():
    live = _load_toy_phase0()
    assert "9" not in live
    changes, _ = load_changes_jsonl(FIXTURES / "toy_changes.jsonl")
    new_live, _ = apply_phase1(live, changes)
    assert "9" in new_live


def test_toy_final_count():
    """
    Toy: 5 unique docs after dedup, 2 DELETE, 1 UPSERT-replace, 1 UPSERT-new
    = 5 - 2 + 1 = 4 live docs.
    """
    live = _load_toy_phase0()
    changes, _ = load_changes_jsonl(FIXTURES / "toy_changes.jsonl")
    new_live, _ = apply_phase1(live, changes)
    assert len(new_live) == 4


def test_idempotent_apply():
    """Applying the same changes twice must produce the same result."""
    live = _load_toy_phase0()
    changes, _ = load_changes_jsonl(FIXTURES / "toy_changes.jsonl")
    once, _ = apply_phase1(live, changes)
    twice, _ = apply_phase1(once, changes)
    assert set(once.keys()) == set(twice.keys())
    for doc_id in once:
        assert once[doc_id] == twice[doc_id]


def test_invalid_change_is_rejected():
    live = {"1": {"doc_id": "1", "url": "u", "title": "t", "text": "x", "links": []}}
    bad_change = {"doc_id": "1", "op": "ZOOP"}  # invalid op
    new_live, rejected = apply_phase1(live, [bad_change])
    assert len(rejected) == 1
    assert "1" in new_live  # original doc preserved


def test_phase1_contract_975_docs():
    """
    Simulate full dataset contract:
    1000 unique Phase-0 docs, 50 DELETE, 25 UPSERT-replace, 25 UPSERT-new
    = 1000 - 50 + 25 = 975.
    """
    base = {"url": "u", "title": "t", "text": "x", "links": []}
    live = {str(i): {"doc_id": str(i), **base} for i in range(1, 1001)}

    changes: list[dict] = []
    # 50 DELETE (IDs 1..50)
    for i in range(1, 51):
        changes.append({"doc_id": str(i), "op": "DELETE"})
    # 25 UPSERT replace (IDs 51..75)
    for i in range(51, 76):
        changes.append({"doc_id": str(i), "op": "UPSERT", "url": "u2",
                        "title": "t2", "text": "x2", "links": []})
    # 25 UPSERT new (IDs 1001..1025)
    for i in range(1001, 1026):
        changes.append({"doc_id": str(i), "op": "UPSERT", "url": "u3",
                        "title": "t3", "text": "x3", "links": []})

    new_live, rejected = apply_phase1(live, changes)
    assert len(rejected) == 0
    assert len(new_live) == 975
