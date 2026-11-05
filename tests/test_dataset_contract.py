"""Small source-schema examples; real corpus files stay out of Git."""
import json


def test_delete_requires_only_doc_id_and_op():
    event = json.loads('{"doc_id": "1", "op": "DELETE"}')
    assert set(event) == {"doc_id", "op"}


def test_links_optional_in_phase_zero():
    row = json.loads('{"doc_id":"3880","url":"https://example.org","title":"x","text":"y"}')
    assert row.get("links", []) == []


def test_phase_one_jsonl_not_json_array():
    changes = '{"doc_id":"1","op":"DELETE"}\n{"doc_id":"2","op":"DELETE"}\n'
    assert [json.loads(line)["doc_id"] for line in changes.splitlines()] == ["1", "2"]
