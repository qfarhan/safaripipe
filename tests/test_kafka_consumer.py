from pathlib import Path
from types import SimpleNamespace

import pytest

from etl import kafka_consumer


class FakeMessage:
    """Duck-types the confluent_kafka.Message methods the consumer uses."""

    def __init__(self, value, *, topic="control-topic", partition=0, offset=0, key=b"k", ts=(1, 123)):
        self._value = value
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._ts = ts

    def error(self):
        return None

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def timestamp(self):
        return self._ts


def test_resolve_consume_options_defaults():
    assert kafka_consumer.resolve_consume_options(
        {}, max_messages=None, poll_timeout_seconds=None
    ) == (100, 10.0)


def test_resolve_consume_options_reads_config_and_validates():
    assert kafka_consumer.resolve_consume_options(
        {"max_messages": 5, "poll_timeout_seconds": 2.0},
        max_messages=None,
        poll_timeout_seconds=None,
    ) == (5, 2.0)
    with pytest.raises(ValueError):
        kafka_consumer.resolve_consume_options({}, max_messages=None, poll_timeout_seconds=0)


def test_message_metadata_adapts_confluent_message():
    meta = kafka_consumer.message_metadata(
        FakeMessage({"event_id": "x"}, partition=2, offset=7, key=b"customer-9", ts=(1, 999))
    )
    assert meta == {
        "topic": "control-topic",
        "partition": 2,
        "offset": 7,
        "timestamp": 999,
        "key": "customer-9",
    }


def test_message_metadata_handles_unavailable_timestamp_and_null_key():
    meta = kafka_consumer.message_metadata(FakeMessage({}, key=None, ts=(0, -1)))
    assert meta["timestamp"] is None
    assert meta["key"] is None


def test_output_path_uses_id_value_and_message_id(tmp_path):
    path = kafka_consumer.output_path(
        tmp_path, {"message_id": "mid", "id_value": "a/b"}
    )
    # the slash in the id must be sanitized so it does not create a subdirectory
    assert path.name == "a_b-mid.json"


def test_process_payload_save_gate(tmp_path, monkeypatch):
    cfg = SimpleNamespace(
        message={
            "id_attribute": "header.batchId",
            "status_field": "control.batch.processStatus",
            "status_value": "End",
        },
        consumer={"output_dir": str(tmp_path), "trigger_next": False},
    )
    monkeypatch.setattr(kafka_consumer, "load_config", lambda env: cfg)

    end_msg = {"header": {"batchId": "b1"}, "control": {"batch": {"processStatus": "End"}}}
    start_msg = {"header": {"batchId": "b2"}, "control": {"batch": {"processStatus": "Start"}}}

    saved = kafka_consumer.process_payload(
        payload=end_msg, env="local", source="t", kafka_metadata=None,
        trigger_next=False, dry_run_next=False,
    )
    assert saved["skipped"] is False
    assert Path(saved["saved_file"]).exists()

    skipped = kafka_consumer.process_payload(
        payload=start_msg, env="local", source="t", kafka_metadata=None,
        trigger_next=False, dry_run_next=False,
    )
    assert skipped["skipped"] is True
    assert skipped["saved_file"] is None

    # Only the End message produced a file; the Start message was gated out.
    assert len(list(tmp_path.glob("*.json"))) == 1


def test_consume_messages_deserializes_and_stops_on_drain(monkeypatch):
    polled = [FakeMessage({"event_id": "one"}), FakeMessage({"event_id": "two"}), None]

    class FakeConsumer:
        def __init__(self):
            self.subscribed = None
            self.closed = False
            self._it = iter(polled)

        def subscribe(self, topics):
            self.subscribed = topics

        def poll(self, timeout):
            return next(self._it)

        def close(self):
            self.closed = True

    fake = FakeConsumer()

    monkeypatch.setattr(
        kafka_consumer,
        "load_config",
        lambda env: SimpleNamespace(
            kafka={"topic": "control-topic", "client": {}, "consumer": {}},
            schema_registry={"url": "http://sr"},
            consumer={"max_messages": 0, "poll_timeout_seconds": 1.0},
        ),
    )
    monkeypatch.setattr(kafka_consumer, "create_kafka_consumer", lambda cfg: fake)
    monkeypatch.setattr(kafka_consumer, "create_schema_registry_client", lambda cfg: object())
    # Fake Avro deserializer: the FakeMessage already carries a dict value.
    monkeypatch.setattr(kafka_consumer, "create_avro_deserializer", lambda sr: (lambda value, ctx: value))
    monkeypatch.setattr(
        kafka_consumer, "process_payload", lambda **kw: {"payload": kw["payload"], "meta": kw["kafka_metadata"]}
    )

    results = list(
        kafka_consumer.consume_messages(env="local", once=True, trigger_next=None, dry_run_next=False)
    )

    assert [r["payload"] for r in results] == [{"event_id": "one"}, {"event_id": "two"}]
    assert fake.subscribed == ["control-topic"]
    assert fake.closed is True
