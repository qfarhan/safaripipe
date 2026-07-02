from etl import control_query
from etl.control_query import build_control_query, find_completed_batches


def test_build_control_query_uses_term_filters():
    # Exact + unscored: both clauses must be term queries in FILTER context —
    # a match query on an analyzed field could silently return wrong batches.
    assert build_control_query(
        action_field="control.action",
        action_value="End",
        date_field="control.eodDate",
        date_value="2026-02-05",
    ) == {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"control.action": "End"}},
                    {"term": {"control.eodDate": "2026-02-05"}},
                ]
            }
        }
    }


def _hit(doc_id, batch_id, size=None):
    source = {"header": {"batchId": batch_id}, "control": {"action": "End"}}
    if size is not None:
        source["control"]["batchSizeIntended"] = size
    return {"_id": doc_id, "_source": source}


def _find(monkeypatch, hits, **overrides):
    captured = {}

    def fake_scan(client, *, index, query, size, scroll_timeout):
        captured.update(index=index, query=query, size=size, scroll_timeout=scroll_timeout)
        return iter(hits)

    monkeypatch.setattr(control_query, "scan_all_hits", fake_scan)
    kwargs = dict(
        control_index="ist.enterprise.c360.facility.eod.control.v1",
        action_field="control.action",
        action_value="End",
        date_field="control.eodDate",
        date_value="2026-02-05",
        batch_id_field="header.batchId",
        batch_size_field="control.batchSizeIntended",
    )
    kwargs.update(overrides)
    return find_completed_batches(object(), **kwargs), captured


def test_find_completed_batches_extracts_ids_and_sizes(monkeypatch):
    batches, captured = _find(
        monkeypatch, [_hit("m1", "C360_A", 10), _hit("m2", "C360_B", 20)]
    )
    assert batches == [
        {"batch_id": "C360_A", "message_id": "m1", "batch_size_intended": 10},
        {"batch_id": "C360_B", "message_id": "m2", "batch_size_intended": 20},
    ]
    # The query must target the alias passed in, fully paged via the scan helper.
    assert captured["index"] == "ist.enterprise.c360.facility.eod.control.v1"
    assert "filter" in captured["query"]["query"]["bool"]


def test_find_completed_batches_dedupes_same_batch_id(monkeypatch):
    # A re-emitted End for the same batchId must collapse to one entry; the
    # LAST doc wins so a corrected batchSizeIntended is the one asserted.
    batches, _ = _find(monkeypatch, [_hit("m1", "C360_A", 10), _hit("m2", "C360_A", 12)])
    assert batches == [
        {"batch_id": "C360_A", "message_id": "m2", "batch_size_intended": 12}
    ]


def test_find_completed_batches_skips_docs_without_join_key(monkeypatch):
    broken = {"_id": "m0", "_source": {"control": {"action": "End"}}}
    batches, _ = _find(monkeypatch, [broken, _hit("m1", "C360_A")])
    assert [b["batch_id"] for b in batches] == ["C360_A"]


def test_find_completed_batches_without_size_field(monkeypatch):
    batches, _ = _find(monkeypatch, [_hit("m1", "C360_A", 10)], batch_size_field=None)
    assert batches == [{"batch_id": "C360_A", "message_id": "m1"}]
