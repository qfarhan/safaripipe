from etl.config import (
    AppConfig,
    consumer_client_config,
    load_config,
    producer_client_config,
)


def _config(**kafka) -> AppConfig:
    return AppConfig(
        env="test",
        kafka=kafka,
        schema_registry={},
        consumer={},
        message={},
        elasticsearch={},
    )


def test_consumer_client_config_merges_client_and_consumer():
    config = _config(
        client={"bootstrap.servers": "h:9092", "security.protocol": "PLAINTEXT"},
        consumer={"group.id": "g", "auto.offset.reset": "earliest"},
    )
    assert consumer_client_config(config) == {
        "bootstrap.servers": "h:9092",
        "security.protocol": "PLAINTEXT",
        "group.id": "g",
        "auto.offset.reset": "earliest",
    }


def test_producer_client_config_excludes_consumer_only_props():
    config = _config(
        client={"bootstrap.servers": "h:9092"},
        consumer={"group.id": "g"},
    )
    # group.id (a consumer-only prop) must NOT leak into the producer config.
    assert producer_client_config(config) == {"bootstrap.servers": "h:9092"}


def test_local_config_is_plaintext_with_schema_registry():
    config = load_config("local")
    assert config.kafka["client"]["security.protocol"] == "PLAINTEXT"
    assert config.kafka["topic"] == "control-topic"
    assert config.schema_registry["url"].startswith("http://")


def test_local_kerberos_config_selects_gssapi():
    config = load_config("local-kerberos")
    client = config.kafka["client"]
    assert client["security.protocol"] == "SASL_PLAINTEXT"
    assert client["sasl.mechanism"] == "GSSAPI"
    assert client["sasl.kerberos.service.name"] == "kafka"
