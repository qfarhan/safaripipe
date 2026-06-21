from types import SimpleNamespace

from etl import kafka_consumer


def test_flatten_polled_records_returns_messages_in_partition_order():
    first = SimpleNamespace(value={"event_id": "one"})
    second = SimpleNamespace(value={"event_id": "two"})

    assert kafka_consumer.flatten_polled_records({"partition-0": [first], "partition-1": [second]}) == [
        first,
        second,
    ]


def test_resolve_batch_options_defaults_to_ten_minute_interval():
    max_records, interval_seconds, poll_timeout_ms = kafka_consumer.resolve_batch_options(
        {},
        batch_max_records=None,
        batch_interval_seconds=None,
        poll_timeout_ms=None,
    )

    assert max_records == 100
    assert interval_seconds == 600
    assert poll_timeout_ms == 1000


def test_consume_messages_waits_between_non_empty_batches(monkeypatch):
    first = SimpleNamespace(
        value={"event_id": "one"},
        topic="control-topic",
        partition=0,
        offset=0,
        timestamp=1,
        key=b"one",
    )
    second = SimpleNamespace(
        value={"event_id": "two"},
        topic="control-topic",
        partition=0,
        offset=1,
        timestamp=2,
        key=b"two",
    )

    class FakeConsumer:
        def __init__(self):
            self.poll_calls = 0
            self.closed = False

        def poll(self, *, timeout_ms, max_records):
            self.poll_calls += 1
            assert timeout_ms == 25
            assert max_records == 1
            if self.poll_calls == 1:
                return {"partition-0": [first]}
            if self.poll_calls == 2:
                return {"partition-0": [second]}
            raise AssertionError("test should only poll two batches")

        def close(self):
            self.closed = True

    fake_consumer = FakeConsumer()
    sleeps: list[float] = []
    monotonic_values = iter([0.0, 1.0, 600.0])

    monkeypatch.setattr(
        kafka_consumer,
        "load_config",
        lambda env: SimpleNamespace(
            kafka={},
            consumer={"batch_max_records": 1, "batch_interval_seconds": 600, "poll_timeout_ms": 25},
        ),
    )
    monkeypatch.setattr(kafka_consumer, "create_kafka_consumer", lambda kafka_config: fake_consumer)
    monkeypatch.setattr(
        kafka_consumer,
        "process_payload",
        lambda **kwargs: {"payload": kwargs["payload"], "metadata": kwargs["kafka_metadata"]},
    )
    monkeypatch.setattr(kafka_consumer.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(kafka_consumer.time, "sleep", sleeps.append)

    results = kafka_consumer.consume_messages(
        env="local",
        once=False,
        trigger_next=None,
        dry_run_next=False,
    )

    assert next(results)["payload"] == {"event_id": "one"}
    assert sleeps == []
    assert next(results)["payload"] == {"event_id": "two"}
    assert sleeps == [599.0]
    results.close()
    assert fake_consumer.closed is True
