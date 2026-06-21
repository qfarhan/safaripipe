from etl.message import convert_control_message


def test_convert_control_message_promotes_configured_id_attribute():
    converted = convert_control_message(
        {"event_id": "abc-123", "name": "Example"},
        id_attribute="event_id",
        source="test",
    )

    assert converted["id_value"] == "abc-123"
    assert converted["payload"]["name"] == "Example"
