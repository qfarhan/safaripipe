from etl.message import convert_control_message, status_matches


def test_convert_control_message_promotes_configured_id_attribute():
    converted = convert_control_message(
        {"event_id": "abc-123", "name": "Example"},
        id_attribute="event_id",
        source="test",
    )

    assert converted["id_value"] == "abc-123"
    assert converted["payload"]["name"] == "Example"


def test_convert_control_message_promotes_nested_id_attribute():
    converted = convert_control_message(
        {"header": {"batchId": "batch-9"}, "body": {}},
        id_attribute="header.batchId",
        source="test",
    )

    assert converted["id_value"] == "batch-9"


def test_status_matches_on_nested_field():
    payload = {"control": {"batch": {"processStatus": "End"}}}
    assert status_matches(payload, "control.batch.processStatus", "End") is True
    assert status_matches(payload, "control.batch.processStatus", "Start") is False


def test_status_matches_missing_field_is_false():
    assert status_matches({"control": {}}, "control.batch.processStatus", "End") is False


def test_status_matches_disabled_when_field_unset():
    # An empty status_field disables the gate, so any message passes.
    assert status_matches({"anything": 1}, "", "End") is True
