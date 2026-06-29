import io
import json

import fastavro

from etl.config import PROJECT_ROOT
from etl.json_io import load_json_file


def test_sample_message_roundtrips_against_avro_schema():
    """The committed sample message must validate against the Avro schema.

    This is an infra-free guard: if the schema and the sample drift apart, the
    producer would fail at runtime. fastavro is the same Avro engine confluent's
    AvroSerializer uses under the hood.
    """
    schema = json.loads((PROJECT_ROOT / "samples" / "control_message.avsc").read_text())
    parsed = fastavro.parse_schema(schema)
    record = load_json_file(PROJECT_ROOT / "samples" / "control_message.json")

    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, parsed, record)
    buffer.seek(0)
    decoded = fastavro.schemaless_reader(buffer, parsed)

    assert decoded["header"]["batchId"] == record["header"]["batchId"]
    assert decoded["control"]["batch"]["processStatus"] == record["control"]["batch"]["processStatus"]
    assert decoded["attributes"]["tenant"] == "demo"


def test_start_sample_roundtrips_against_avro_schema():
    """The Start (skipped) sample must also validate against the same schema."""
    schema = json.loads((PROJECT_ROOT / "samples" / "control_message.avsc").read_text())
    parsed = fastavro.parse_schema(schema)
    record = load_json_file(PROJECT_ROOT / "samples" / "control_message_start.json")

    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, parsed, record)
    buffer.seek(0)
    decoded = fastavro.schemaless_reader(buffer, parsed)

    assert decoded["control"]["batch"]["processStatus"] == "Start"
