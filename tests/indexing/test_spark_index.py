"""Tests for Spark indexing helper functions (no cluster required)."""

from distributed_search.indexing.spark_index import (
    _postings_for_document,
    _document_length,
)


def test_repeated_terms():
    result = _postings_for_document(
        ("1", "apple apple banana")
    )
    assert sorted(result) == [
        ("apple", "1", 2),
        ("banana", "1", 1),
    ]


def test_document_length():
    assert _document_length(
        ("1", "apple apple banana")
    ) == ("1", 3)


def test_empty_document():
    assert _postings_for_document(("2", "")) == []
    assert _document_length(("2", "")) == ("2", 0)


def test_null_text():
    assert _postings_for_document(("3", None)) == []
    assert _document_length(("3", None)) == ("3", 0)


def test_shared_tokenizer():
    result = _postings_for_document(
        ("4", "Hello [[World|Earth]] HELLO!")
    )
    assert sorted(result) == [
        ("earth", "4", 1),
        ("hello", "4", 2),
    ]


def test_document_ids_are_strings():
    assert _postings_for_document(
        (42, "test")
    ) == [("test", "42", 1)]
