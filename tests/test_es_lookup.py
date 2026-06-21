from etl.es_lookup import build_query_from_id, extract_id_from_message, normalize_query


def test_build_query_from_id():
    assert build_query_from_id("event_id", "abc-123") == {
        "query": {"term": {"event_id": "abc-123"}}
    }


def test_extract_id_from_converted_message():
    assert extract_id_from_message({"id_value": "abc-123"}, "event_id") == "abc-123"


def test_normalize_query_wraps_clause():
    assert normalize_query({"term": {"event_id": "abc-123"}}) == {
        "query": {"term": {"event_id": "abc-123"}}
    }
