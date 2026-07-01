import json
from types import SimpleNamespace

import pytest

from etl import es_lookup
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


def _config(*, term_field=None):
    es = {"index": "source-index"}
    if term_field is not None:
        es["term_field"] = term_field
    return SimpleNamespace(
        message={"id_attribute": "header.batchId"},
        elasticsearch=es,
    )


def test_run_lookup_uses_term_field_for_query(monkeypatch):
    # When the index maps batchId as text+keyword, the term query targets the subfield.
    monkeypatch.setattr(es_lookup, "load_config", lambda env: _config(term_field="header.batchId.keyword"))
    result = es_lookup.run_lookup(env="local", direct_id="batch-9", dry_run=True)
    assert result["query"] == {"query": {"term": {"header.batchId.keyword": "batch-9"}}}


def test_run_lookup_term_field_defaults_to_id_attribute(monkeypatch):
    monkeypatch.setattr(es_lookup, "load_config", lambda env: _config())
    result = es_lookup.run_lookup(env="local", direct_id="batch-9", dry_run=True)
    assert result["query"] == {"query": {"term": {"header.batchId": "batch-9"}}}


def test_write_hits_jsonl_writes_one_doc_per_line(tmp_path):
    path = tmp_path / "out.jsonl"
    count = es_lookup.write_hits_jsonl(path, [{"_id": "1"}, {"_id": "2"}, {"_id": "3"}])

    assert count == 3
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    assert [json.loads(line)["_id"] for line in lines] == ["1", "2", "3"]


def test_run_lookup_scrolls_all_hits_and_writes_file(monkeypatch, tmp_path):
    # Simulate a batch with more matching records than a single search page
    # (default size 10 / max_result_window 10000) would ever return.
    fake_hits = [{"_id": str(i), "batchId": "batch-9"} for i in range(25)]

    monkeypatch.setattr(es_lookup, "load_config", lambda env: _config())
    monkeypatch.setattr(es_lookup, "create_es_client", lambda es_config: object())
    monkeypatch.setattr(
        es_lookup,
        "scan_all_hits",
        lambda client, *, index, query, size, scroll_timeout: iter(fake_hits),
    )

    output_file = tmp_path / "results.jsonl"
    result = es_lookup.run_lookup(
        env="local", direct_id="batch-9", output_file=str(output_file)
    )

    assert result["record_count"] == 25
    assert result["output_file"] == str(output_file)
    assert "response" not in result
    written = [json.loads(line) for line in output_file.read_text(encoding="utf-8").splitlines()]
    assert written == fake_hits
