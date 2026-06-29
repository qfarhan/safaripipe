import pytest

from etl.es_lookup import build_query_from_id, extract_id_from_message, normalize_query


def test_build_query_from_id():
    assert build_query_from_id("event_id", "abc-123") == {
        "query": {"term": {"event_id": "abc-123"}}
    }


def test_build_query_from_nested_id():
    # A dotted path is exactly how ES addresses an object subfield in a term query.
    assert build_query_from_id("header.batchId", "batch-9") == {
        "query": {"term": {"header.batchId": "batch-9"}}
    }


def test_extract_id_from_converted_message():
    assert extract_id_from_message({"id_value": "abc-123"}, "event_id") == "abc-123"


def test_extract_nested_id_from_payload():
    message = {"payload": {"header": {"batchId": "batch-9"}, "body": {}}}
    assert extract_id_from_message(message, "header.batchId") == "batch-9"


def test_extract_nested_id_from_top_level():
    message = {"header": {"batchId": "batch-9"}}
    assert extract_id_from_message(message, "header.batchId") == "batch-9"


def test_extract_missing_nested_id_raises():
    with pytest.raises(KeyError):
        extract_id_from_message({"header": {}}, "header.batchId")


def test_normalize_query_wraps_clause():
    assert normalize_query({"term": {"event_id": "abc-123"}}) == {
        "query": {"term": {"event_id": "abc-123"}}
    }
