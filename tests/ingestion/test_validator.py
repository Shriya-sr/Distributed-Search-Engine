"""tests/ingestion/test_validator.py"""
import pytest
from distributed_search.ingestion import validate_page, validate_change


# ── Phase 0 page validation ────────────────────────────────────────────────

def test_valid_page_passes():
    rec = {"doc_id": "1", "url": "u", "title": "t", "text": "x", "links": ["2"]}
    ok, reason = validate_page(rec)
    assert ok, reason


def test_valid_page_without_links_passes():
    rec = {"doc_id": "1", "url": "u", "title": "t", "text": "x"}
    ok, reason = validate_page(rec)
    assert ok, reason


def test_page_missing_title_fails():
    rec = {"doc_id": "1", "url": "u", "text": "x"}
    ok, _ = validate_page(rec)
    assert not ok


def test_page_non_string_doc_id_fails():
    rec = {"doc_id": 1, "url": "u", "title": "t", "text": "x"}
    ok, _ = validate_page(rec)
    assert not ok


def test_page_links_not_list_fails():
    rec = {"doc_id": "1", "url": "u", "title": "t", "text": "x", "links": "2"}
    ok, _ = validate_page(rec)
    assert not ok


# ── Phase 1 change validation ──────────────────────────────────────────────

def test_valid_delete_passes():
    rec = {"doc_id": "1", "op": "DELETE"}
    ok, reason = validate_change(rec)
    assert ok, reason


def test_delete_does_not_need_text():
    rec = {"doc_id": "1", "op": "DELETE"}
    ok, _ = validate_change(rec)
    assert ok


def test_valid_upsert_passes():
    rec = {"doc_id": "9", "op": "UPSERT", "url": "u", "title": "t", "text": "x"}
    ok, reason = validate_change(rec)
    assert ok, reason


def test_upsert_missing_url_fails():
    rec = {"doc_id": "9", "op": "UPSERT", "title": "t", "text": "x"}
    ok, _ = validate_change(rec)
    assert not ok


def test_invalid_op_fails():
    rec = {"doc_id": "1", "op": "MODIFY"}
    ok, _ = validate_change(rec)
    assert not ok


def test_missing_doc_id_fails():
    rec = {"op": "DELETE"}
    ok, _ = validate_change(rec)
    assert not ok
