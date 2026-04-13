import json
import time
from datetime import datetime, timedelta, timezone

import pytest

import ray
from ray.data._internal.io.datasink.kafka_datasink import KafkaDatasink
from ray.data._internal.io.datasource.kafka_datasource import (
    KafkaAuthConfig,
    _build_consumer_config_for_read,
    _datetime_to_ms,
)

pytest.importorskip("confluent_kafka")


@pytest.fixture(scope="session")
def kafka_container():
    from testcontainers.kafka import KafkaContainer

    print("\nStarting Kafka container (shared across all tests)...")
    with KafkaContainer() as kafka:
        bootstrap_server = kafka.get_bootstrap_server()
        print(f"Kafka container started at {bootstrap_server}")
        yield kafka
        print("\nShutting down Kafka container...")


@pytest.fixture(scope="session")
def bootstrap_server(kafka_container):
    return kafka_container.get_bootstrap_server()


def _json_value_serializer(obj, ctx):
    return json.dumps(obj).encode("utf-8")


def _str_key_serializer(obj, ctx):
    return obj.encode("utf-8") if obj else None


@pytest.fixture(scope="session")
def kafka_producer(bootstrap_server):
    from confluent_kafka.serializing_producer import SerializingProducer

    print(f"Creating shared Kafka producer for {bootstrap_server}")
    producer = SerializingProducer(
        {
            "bootstrap.servers": bootstrap_server,
            "value.serializer": _json_value_serializer,
            "key.serializer": _str_key_serializer,
        }
    )
    yield producer
    producer.flush()
    print("Closing shared Kafka producer")
    producer.close()


@pytest.fixture(scope="session")
def kafka_consumer(bootstrap_server):
    from confluent_kafka import Consumer

    print(f"Creating shared Kafka consumer for {bootstrap_server}")
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_server,
            "group.id": "ray-test-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    yield consumer
    print("Closing shared Kafka consumer")
    consumer.close()


def consume_messages(consumer, topic, expected_count, timeout=10):
    """Helper function to consume messages from a topic."""
    from confluent_kafka import KafkaError, TopicPartition

    # Discover partitions for the topic
    metadata = consumer.list_topics(timeout=10)
    topic_meta = metadata.topics.get(topic)
    if topic_meta is None or not topic_meta.partitions:
        time.sleep(2)  # Wait a bit more for topic to be created
        metadata = consumer.list_topics(timeout=10)
        topic_meta = metadata.topics.get(topic)

    if topic_meta and topic_meta.partitions:
        topic_partitions = [
            TopicPartition(topic, p, 0) for p in topic_meta.partitions.keys()
        ]
        consumer.assign(topic_partitions)

    messages = []
    start_time = time.time()

    while len(messages) < expected_count and (time.time() - start_time) < timeout:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            break
        messages.append(msg)

    return messages


def test_build_consumer_config_for_read():
    """Test read config builder."""
    bootstrap_servers = ["localhost:9092"]

    # Test basic config
    config = _build_consumer_config_for_read(bootstrap_servers, None)
    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["enable.auto.commit"] is False
    assert "group.id" in config

    # Test with authentication via consumer_config
    user_conf = {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "user",
        "sasl.password": "pass",
    }
    config_with_auth = _build_consumer_config_for_read(
        bootstrap_servers, None, user_conf
    )
    assert config_with_auth["security.protocol"] == "SASL_SSL"
    assert config_with_auth["sasl.mechanism"] == "PLAIN"
    assert config_with_auth["sasl.username"] == "user"
    assert config_with_auth["sasl.password"] == "pass"


def test_build_consumer_config_with_pass_through():
    """Test that extra consumer_config options pass through and cannot override bootstrap.servers."""
    bootstrap_servers = ["localhost:9092"]

    # Extra options should pass through
    extra = {
        "ssl.endpoint.identification.algorithm": "none",
        "group.id": "custom-group",
        "enable.auto.commit": True,
    }
    config = _build_consumer_config_for_read(bootstrap_servers, None, extra)
    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["ssl.endpoint.identification.algorithm"] == "none"
    assert config["group.id"] == "custom-group"
    assert config["enable.auto.commit"] is True

    # Attempt to override bootstrap.servers should be ignored
    override = {"bootstrap.servers": "override:9092"}
    config2 = _build_consumer_config_for_read(bootstrap_servers, None, override)
    assert config2["bootstrap.servers"] == "localhost:9092"


def test_read_kafka_config_conflict_raises():
    """Specifying both kafka_auth_config and consumer_config should error."""
    with pytest.raises(
        ValueError, match="Provide only one of kafka_auth_config.* or consumer_config"
    ):
        ray.data.read_kafka(
            topics="t",
            bootstrap_servers="localhost:9092",
            kafka_auth_config=KafkaAuthConfig(security_protocol="SSL"),
            consumer_config={"security.protocol": "SSL"},
        )


def test_build_consumer_config_with_kafka_auth_config_deprecated():
    """Test kafka-python style KafkaAuthConfig mapping (deprecated path)."""
    bootstrap_servers = ["localhost:9092"]
    auth = KafkaAuthConfig(
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username="testuser",
        sasl_plain_password="testpass",
        sasl_kerberos_name="kafka/hostname@REALM",
        sasl_kerberos_service_name="kafka",
        # These are unsupported and should be ignored with warnings
        sasl_kerberos_domain_name="example.com",
        sasl_oauth_token_provider=object(),
        ssl_context=object(),
        # ssl_check_hostname False is unsafe to map; ensure not weakening
        ssl_check_hostname=False,
        # SSL files
        ssl_cafile="/path/to/ca.pem",
        ssl_certfile="/path/to/cert.pem",
        ssl_keyfile="/path/to/key.pem",
        ssl_password="keypassword",
        ssl_ciphers="HIGH:!aNULL",
        ssl_crlfile="/path/to/crl.pem",
    )
    config = _build_consumer_config_for_read(bootstrap_servers, auth, None)
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "SCRAM-SHA-256"
    assert config["sasl.username"] == "testuser"
    assert config["sasl.password"] == "testpass"
    assert config["sasl.kerberos.principal"] == "kafka/hostname@REALM"
    assert config["sasl.kerberos.service.name"] == "kafka"
    # No weakening of TLS verification
    assert "enable.ssl.certificate.verification" not in config
    assert config["ssl.ca.location"] == "/path/to/ca.pem"
    assert config["ssl.certificate.location"] == "/path/to/cert.pem"
    assert config["ssl.key.location"] == "/path/to/key.pem"
    assert config["ssl.key.password"] == "keypassword"
    assert config["ssl.cipher.suites"] == "HIGH:!aNULL"
    assert config["ssl.crl.location"] == "/path/to/crl.pem"


def test_datetime_to_ms_without_timezone():
    """Test that datetimes without timezone info are treated as UTC."""
    assert _datetime_to_ms(datetime(1970, 1, 1, 0, 0, 0)) == 0
    assert _datetime_to_ms(datetime(2025, 1, 1, 0, 0, 0)) == 1735689600000


def test_datetime_to_ms_with_timezone():
    """Test that timezone-aware datetimes are converted to UTC correctly."""
    from datetime import timezone

    assert _datetime_to_ms(datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)) == 0
    assert (
        _datetime_to_ms(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))
        == 1735689600000
    )


def test_read_kafka_datetime_validation():
    """Test that start_offset > end_offset with datetimes raises ValueError."""
    with pytest.raises(ValueError, match="start_offset must be less than end_offset"):
        ray.data.read_kafka(
            topics="test-topic",
            bootstrap_servers="localhost:9092",
            start_offset=datetime(2025, 6, 1),
            end_offset=datetime(2025, 1, 1),
        )


# Integration Tests (require Kafka container)


def test_read_kafka_basic(bootstrap_server, kafka_producer, ray_start_regular_shared):
    topic = "test-basic"

    for i in range(100):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.produce(topic, value=message, key=f"key-{i}")
    kafka_producer.flush()

    # Wait a bit for messages to be committed
    time.sleep(0.3)

    # Read from Kafka
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
    )

    # Verify data
    records = ds.take_all()
    assert len(records) == 100

    # Check first record structure
    first_record = records[0]
    assert "offset" in first_record
    assert "key" in first_record
    assert "value" in first_record
    assert "topic" in first_record
    assert "partition" in first_record
    assert "timestamp" in first_record
    assert first_record["topic"] == topic

    # Verify data types: key is bytes, value is binary
    assert isinstance(first_record["key"], bytes)
    assert isinstance(first_record["value"], bytes)
    key_str = first_record["key"].decode("utf-8")
    assert key_str.startswith("key-")
    value_obj = json.loads(first_record["value"].decode("utf-8"))
    assert "id" in value_obj
    assert "value" in value_obj


@pytest.mark.parametrize(
    "total_messages,start_offset,end_offset,expected_count,test_id",
    [
        (100, 20, 80, 60, "both-set"),
        (100, 50, None, 50, "start-offset-only"),
        (100, None, 50, 50, "end-offset-only"),
        (100, None, None, 100, "both-none"),
        (100, "earliest", 30, 30, "earliest-start-offset-number-end-offset"),
        (100, 50, "latest", 50, "number-start-offset-number-end-offset"),
    ],
)
def test_read_kafka_with_offsets(
    bootstrap_server,
    kafka_producer,
    ray_start_regular_shared,
    total_messages,
    start_offset,
    end_offset,
    expected_count,
    test_id,
):
    topic = f"test-{test_id}"

    for i in range(total_messages):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.produce(topic, value=message)
    kafka_producer.flush()
    time.sleep(0.3)

    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset=start_offset,
        end_offset=end_offset,
    )

    records = ds.take_all()
    assert len(records) == expected_count


def test_read_kafka_multiple_partitions(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    from confluent_kafka.admin import AdminClient, NewTopic

    topic = "test-multi-partition"

    # Create topic with 3 partitions
    admin_client = AdminClient({"bootstrap.servers": bootstrap_server})
    topic_config = NewTopic(topic, num_partitions=3, replication_factor=1)
    admin_client.create_topics([topic_config])

    time.sleep(2)  # Wait for topic creation

    # Send messages to different partitions
    for i in range(150):
        message = {"id": i, "value": f"message-{i}"}
        partition = i % 3
        kafka_producer.produce(topic, value=message, partition=partition)
    kafka_producer.flush()
    time.sleep(0.3)

    # Read from all partitions
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
    )

    records = ds.take_all()
    assert len(records) == 150


def test_read_kafka_multiple_topics(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    topic1 = "test-multi-topic-1"
    topic2 = "test-multi-topic-2"

    # Send messages to topic1
    for i in range(50):
        message = {"id": i, "value": f"topic1-message-{i}"}
        kafka_producer.produce(topic1, value=message)

    # Send messages to topic2
    for i in range(30):
        message = {"id": i, "value": f"topic2-message-{i}"}
        kafka_producer.produce(topic2, value=message)

    kafka_producer.flush()
    time.sleep(0.3)

    # Read from both topics
    ds = ray.data.read_kafka(
        topics=[topic1, topic2],
        bootstrap_servers=[bootstrap_server],
    )

    records = ds.take_all()
    assert len(records) == 80


def test_read_kafka_with_message_headers(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    topic = "test-headers"

    for i in range(10):
        message = {"id": i, "value": f"message-{i}"}
        headers = [
            ("header1", b"value1"),
            ("header2", f"value-{i}".encode("utf-8")),
        ]
        kafka_producer.produce(topic, value=message, headers=headers)
    kafka_producer.flush()
    time.sleep(0.3)

    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
    )

    records = ds.take_all()
    assert len(records) == 10

    first_record = records[0]
    assert "headers" in first_record
    assert isinstance(first_record["headers"]["header1"], bytes)
    assert first_record["headers"]["header1"].decode("utf-8") == "value1"


@pytest.mark.parametrize(
    "start_offset,end_offset,expected_count,test_id",
    [
        (0, 150, 100, "end-offset-exceeds-available-messages"),
        (
            "earliest",
            150,
            100,
            "earliest-start-offset-end-offset-exceeds-available-messages",
        ),
    ],
)
def test_read_kafka_offset_exceeds_available_messages(
    bootstrap_server,
    kafka_producer,
    ray_start_regular_shared,
    start_offset,
    end_offset,
    expected_count,
    test_id,
):
    topic = f"test-offset-timeout-{test_id}"

    for i in range(100):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.produce(topic, value=message)
    kafka_producer.flush()
    time.sleep(0.3)

    # end_offset exceeds available messages, but it gets clamped to the high
    # watermark during offset resolution, so the read completes without
    # hanging.
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset=start_offset,
        end_offset=end_offset,
    )

    records = ds.take_all()

    assert len(records) == expected_count


def test_read_kafka_default_no_timeout(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    topic = "test-default-no-timeout"

    for i in range(50):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.produce(topic, value=message)
    kafka_producer.flush()
    time.sleep(0.3)

    # timeout_ms is intentionally omitted (defaults to None).
    # Verifies the no-timeout code path works correctly.
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset=0,
        end_offset=50,
    )

    records = ds.take_all()

    assert len(records) == 50


def test_read_kafka_invalid_topic(bootstrap_server, ray_start_regular_shared):
    with pytest.raises(ValueError, match="has no partitions or doesn't exist"):
        ds = ray.data.read_kafka(
            topics=["non-existent-topic"],
            bootstrap_servers=[bootstrap_server],
        )
        ds.take_all()


@pytest.mark.parametrize(
    "start_offset,end_offset,expected_error,topic",
    [
        (0, "earliest", "end_offset cannot be 'earliest'", "test-invalid-offsets-0"),
        ("latest", 1000, "start_offset cannot be 'latest'", "test-invalid-offsets-1"),
        (80, 20, "start_offset must be less than end_offset", "test-invalid-offsets-2"),
        (
            150,
            "latest",
            r"start_offset \(150\) > end_offset \(latest \(resolved to 100\)\) for partition 0 in topic test-invalid-offsets-3",
            "test-invalid-offsets-3",
        ),
        (
            150,
            200,
            r"start_offset \(150\) > end_offset \(200 \(resolved to 100\)\) for partition 0 in topic test-invalid-offsets-4",
            "test-invalid-offsets-4",
        ),
    ],
)
def test_read_kafka_invalid_offsets(
    bootstrap_server,
    kafka_producer,
    ray_start_regular_shared,
    start_offset,
    end_offset,
    expected_error,
    topic,
):
    for i in range(100):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.produce(topic, value=message)
    kafka_producer.flush()
    time.sleep(0.3)

    with pytest.raises(ValueError, match=expected_error):
        ds = ray.data.read_kafka(
            topics=[topic],
            bootstrap_servers=[bootstrap_server],
            start_offset=start_offset,
            end_offset=end_offset,
        )
        ds.take_all()


def test_read_kafka_with_datetime_offsets(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Test reading Kafka messages using datetime-based start and end offsets."""
    topic = "test-datetime-offsets"

    now = datetime.now(timezone.utc)
    time_before = now - timedelta(hours=1)
    time_after = now + timedelta(hours=1)
    msg_ts = _datetime_to_ms(now)

    for i in range(3):
        kafka_producer.produce(topic, value={"id": i}, timestamp=msg_ts)
    kafka_producer.flush()
    # Brief wait for consumer-side metadata propagation after flush()
    time.sleep(0.3)

    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset=time_before,
        end_offset=time_after,
    )
    records = ds.take_all()
    assert len(records) == 3


def test_read_kafka_datetime_partial_range(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Test that only messages within the datetime range are returned."""
    topic = "test-datetime-partial-range"

    # First batch at Jan 1 2025, second batch at Feb 1 2025
    batch1_ts = _datetime_to_ms(datetime(2025, 1, 1))
    batch2_ts = _datetime_to_ms(datetime(2025, 2, 1))
    boundary_time = datetime(2025, 1, 15)  # Between the two batches

    kafka_producer.produce(
        topic,
        value={"batch": 1, "id": 0},
        timestamp=batch1_ts,
    )
    kafka_producer.produce(
        topic,
        value={"batch": 1, "id": 1},
        timestamp=batch1_ts,
    )
    kafka_producer.produce(
        topic,
        value={"batch": 2, "id": 0},
        timestamp=batch2_ts,
    )
    kafka_producer.produce(
        topic,
        value={"batch": 2, "id": 1},
        timestamp=batch2_ts,
    )
    kafka_producer.flush()
    # Brief wait for consumer-side metadata propagation after flush()
    time.sleep(0.3)

    # Read only the second batch using boundary_time as start
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset=boundary_time,
        end_offset="latest",
    )
    records = ds.take_all()
    assert len(records) == 2

    # Read only the first batch using boundary_time as end
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset="earliest",
        end_offset=boundary_time,
    )
    records = ds.take_all()
    assert len(records) == 2


def test_read_kafka_datetime_after_all_messages(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Test datetime start_offset after all messages returns 0 rows."""
    topic = "test-datetime-after-all"

    kafka_producer.produce(topic, value={"id": 0})
    kafka_producer.flush()
    # Brief wait for consumer-side metadata propagation after flush()
    time.sleep(0.3)

    future_time = datetime(2099, 1, 1)

    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset=future_time,
        end_offset="latest",
    )
    records = ds.take_all()
    assert len(records) == 0


def test_read_kafka_datetime_before_all_messages(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Test datetime end_offset before all messages returns 0 rows."""
    topic = "test-datetime-before-all"

    kafka_producer.produce(topic, value={"id": 0})
    kafka_producer.flush()
    # Brief wait for consumer-side metadata propagation after flush()
    time.sleep(0.3)

    past_time = datetime(1970, 1, 2)

    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset="earliest",
        end_offset=past_time,
    )
    records = ds.take_all()
    # All messages have timestamps after 1970, so offsets_for_times will
    # return the first offset. This means end_offset resolves to the
    # beginning, yielding 0 rows.
    assert len(records) == 0


# Kafka Datasink Unit Tests


def test_kafka_datasink_initialization():
    """Test KafkaDatasink initialization and validation."""
    # Valid initialization
    sink = KafkaDatasink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        key_field="id",
        key_serializer="string",
        value_serializer="json",
    )
    assert sink.topic == "test-topic"
    assert sink.bootstrap_servers == "localhost:9092"
    assert sink.key_field == "id"
    assert sink.key_serializer == "string"
    assert sink.value_serializer == "json"

    # Invalid key serializer
    with pytest.raises(ValueError, match="key_serializer must be one of"):
        KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            key_serializer="invalid",
        )

    # Invalid value serializer
    with pytest.raises(ValueError, match="value_serializer must be one of"):
        KafkaDatasink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            value_serializer="invalid",
        )


def test_kafka_datasink_row_to_dict():
    """Test _row_to_dict conversion for different row types."""
    sink = KafkaDatasink(topic="test", bootstrap_servers="localhost:9092")

    # Test with dict
    dict_row = {"a": 1, "b": 2}
    assert sink._row_to_dict(dict_row) == dict_row

    # Test with object that has as_pydict
    class MockArrowRow:
        def as_pydict(self):
            return {"x": 10, "y": 20}

    arrow_row = MockArrowRow()
    assert sink._row_to_dict(arrow_row) == {"x": 10, "y": 20}

    # Test with NamedTuple
    from collections import namedtuple

    Point = namedtuple("Point", ["x", "y"])
    point = Point(x=5, y=10)
    result = sink._row_to_dict(point)
    assert result == {"x": 5, "y": 10}

    # Test with primitive
    assert sink._row_to_dict("string") == "string"
    assert sink._row_to_dict(123) == 123


def test_kafka_datasink_serialize_value():
    """Test value serialization for different formats."""
    # JSON serializer
    sink_json = KafkaDatasink(
        topic="test", bootstrap_servers="localhost:9092", value_serializer="json"
    )
    result = sink_json._serialize_value({"key": "value"})
    assert result == b'{"key": "value"}'

    # String serializer
    sink_string = KafkaDatasink(
        topic="test", bootstrap_servers="localhost:9092", value_serializer="string"
    )
    result = sink_string._serialize_value({"key": "value"})
    assert result == b"{'key': 'value'}"

    # Bytes serializer
    sink_bytes = KafkaDatasink(
        topic="test", bootstrap_servers="localhost:9092", value_serializer="bytes"
    )
    result = sink_bytes._serialize_value(b"raw bytes")
    assert result == b"raw bytes"


def test_kafka_datasink_serialize_key():
    """Test key serialization for different formats."""
    # JSON serializer
    sink_json = KafkaDatasink(
        topic="test", bootstrap_servers="localhost:9092", key_serializer="json"
    )
    result = sink_json._serialize_key({"id": 123})
    assert result == b'{"id": 123}'

    # String serializer (default)
    sink_string = KafkaDatasink(
        topic="test", bootstrap_servers="localhost:9092", key_serializer="string"
    )
    result = sink_string._serialize_key(456)
    assert result == b"456"

    # Bytes serializer
    sink_bytes = KafkaDatasink(
        topic="test", bootstrap_servers="localhost:9092", key_serializer="bytes"
    )
    result = sink_bytes._serialize_key(b"key-bytes")
    assert result == b"key-bytes"


def test_kafka_datasink_extract_key():
    """Test key extraction from rows."""
    sink = KafkaDatasink(
        topic="test",
        bootstrap_servers="localhost:9092",
        key_field="user_id",
        key_serializer="string",
    )

    # Test with dict containing key_field
    row = {"user_id": 123, "name": "Alice"}
    key = sink._extract_key(row)
    assert key == b"123"

    # Test with dict without key_field
    row_no_key = {"name": "Bob"}
    key = sink._extract_key(row_no_key)
    assert key is None

    # Test with no key_field configured
    sink_no_key = KafkaDatasink(topic="test", bootstrap_servers="localhost:9092")
    key = sink_no_key._extract_key({"user_id": 456})
    assert key is None


def test_kafka_datasink_extract_key_uses_serializer():
    """Test that _extract_key properly uses the configured key_serializer."""
    # JSON serializer should produce valid JSON
    sink_json = KafkaDatasink(
        topic="test",
        bootstrap_servers="localhost:9092",
        key_field="key",
        key_serializer="json",
    )
    # Test with dict key value - JSON uses double quotes, str() uses single quotes
    row_dict = {"key": {"nested": "value"}, "data": "test"}
    key = sink_json._extract_key(row_dict)
    assert key == b'{"nested": "value"}'  # JSON format with double quotes

    # Test with string that needs JSON escaping
    row_str = {"key": 'hello "world"', "data": "test"}
    key = sink_json._extract_key(row_str)
    assert key == b'"hello \\"world\\""'  # Properly JSON-escaped

    # Bytes serializer should preserve bytes
    sink_bytes = KafkaDatasink(
        topic="test",
        bootstrap_servers="localhost:9092",
        key_field="key",
        key_serializer="bytes",
    )
    row_bytes = {"key": b"raw-bytes", "data": "test"}
    key = sink_bytes._extract_key(row_bytes)
    assert key == b"raw-bytes"


@pytest.fixture
def mock_write_env():
    from types import SimpleNamespace
    from unittest.mock import MagicMock, patch

    sink = KafkaDatasink(topic="test-topic", bootstrap_servers="localhost:9092")
    mock_producer = MagicMock()
    mock_producer.flush.return_value = 0

    mock_block = MagicMock()
    mock_accessor = MagicMock()
    mock_accessor.iter_rows.return_value = [{"id": 1}]

    with (
        patch("confluent_kafka.Producer", return_value=mock_producer),
        patch(
            "ray.data._internal.io.datasink.kafka_datasink.BlockAccessor.for_block",
            return_value=mock_accessor,
        ),
    ):
        yield SimpleNamespace(
            sink=sink,
            producer=mock_producer,
            block=mock_block,
            accessor=mock_accessor,
            ctx=MagicMock(),
        )


def test_write_delivery_failure_raises(mock_write_env):
    """Test that any delivery failure raises RuntimeError."""
    from unittest.mock import MagicMock

    from confluent_kafka import KafkaError

    env = mock_write_env
    mock_kafka_error = MagicMock(spec=KafkaError)
    mock_kafka_error.str.return_value = "MSG_SIZE_TOO_LARGE"

    def fake_produce(topic, value, key, on_delivery):
        on_delivery(mock_kafka_error, None)

    env.producer.produce.side_effect = fake_produce

    with pytest.raises(RuntimeError, match="Failed to write 1 out of 1"):
        env.sink.write([env.block], ctx=env.ctx)


def test_write_in_flight_after_flush_raises(mock_write_env):
    """Test that messages stuck in-flight after flush raises RuntimeError."""
    env = mock_write_env
    env.producer.produce.return_value = None
    env.producer.flush.return_value = 1  # 1 message still in-flight

    with pytest.raises(
        RuntimeError,
        match="1 out of 1 messages were still in-flight after flush timeout",
    ):
        env.sink.write([env.block], ctx=env.ctx)


def test_write_buffer_error_retry(mock_write_env):
    """Test that BufferError triggers poll and retry, then succeeds."""
    env = mock_write_env
    # First produce() raises BufferError, retry succeeds
    env.producer.produce.side_effect = [BufferError("queue full"), None]

    result = env.sink.write([env.block], ctx=env.ctx)

    assert result["total_records"] == 1
    assert result["failed_messages"] == 0


def test_write_buffer_error_persistent_raises(mock_write_env):
    """Test that persistent BufferError (retry also fails) raises RuntimeError."""
    env = mock_write_env
    env.producer.produce.side_effect = BufferError("queue full")

    with pytest.raises(RuntimeError, match="producer queue is still full"):
        env.sink.write([env.block], ctx=env.ctx)


def test_write_success_returns_stats(mock_write_env):
    """Test successful write returns correct stats."""
    env = mock_write_env
    env.producer.produce.return_value = None
    env.accessor.iter_rows.return_value = [{"id": i} for i in range(3)]

    result = env.sink.write([env.block], ctx=env.ctx)

    assert result == {"total_records": 3, "failed_messages": 0}


# Kafka Datasink Integration Tests (require Kafka container)


def test_write_kafka_basic(bootstrap_server, kafka_consumer, ray_start_regular_shared):
    """Test basic write to Kafka."""
    topic = "test-write-basic"

    # Create dataset
    ds = ray.data.range(100)

    # Write to Kafka
    ds.write_kafka(topic=topic, bootstrap_servers=bootstrap_server)

    # Consume and verify
    messages = consume_messages(kafka_consumer, topic, expected_count=100)
    assert len(messages) == 100

    # Verify message structure
    first_msg = messages[0]
    assert first_msg.topic() == topic
    assert first_msg.key() is None  # No key field specified

    # Verify value is JSON encoded
    value = json.loads(first_msg.value().decode("utf-8"))
    assert "id" in value
    assert isinstance(value["id"], int)


def test_write_kafka_with_keys(
    bootstrap_server, kafka_consumer, ray_start_regular_shared
):
    """Test writing to Kafka with keys."""
    topic = "test-write-with-keys"

    # Create dataset with id field
    data = [{"id": i, "name": f"user-{i}", "value": i * 10} for i in range(50)]
    ds = ray.data.from_items(data)

    # Write to Kafka with id as key
    ds.write_kafka(
        topic=topic,
        bootstrap_servers=bootstrap_server,
        key_field="id",
        key_serializer="string",
    )

    # Consume and verify
    messages = consume_messages(kafka_consumer, topic, expected_count=50)
    assert len(messages) == 50

    # Verify keys
    for msg in messages:
        assert msg.key() is not None
        key_str = msg.key().decode("utf-8")
        assert key_str.isdigit()

        # Verify value
        value = json.loads(msg.value().decode("utf-8"))
        assert value["id"] == int(key_str)


@pytest.mark.parametrize(
    "key_serializer,value_serializer",
    [
        ("string", "json"),
        ("json", "string"),
        ("string", "string"),
        ("json", "json"),
    ],
)
def test_write_kafka_serializers(
    bootstrap_server,
    kafka_consumer,
    ray_start_regular_shared,
    key_serializer,
    value_serializer,
):
    """Test different serializer combinations."""
    topic = f"test-serializers-{key_serializer}-{value_serializer}"

    # Create dataset
    data = [{"id": i, "message": f"msg-{i}"} for i in range(20)]
    ds = ray.data.from_items(data)

    # Write with specified serializers
    ds.write_kafka(
        topic=topic,
        bootstrap_servers=bootstrap_server,
        key_field="id",
        key_serializer=key_serializer,
        value_serializer=value_serializer,
    )

    # Consume and verify
    messages = consume_messages(kafka_consumer, topic, expected_count=20)
    assert len(messages) == 20

    # Verify first message can be deserialized correctly
    first_msg = messages[0]

    if key_serializer == "json":
        key_data = json.loads(first_msg.key().decode("utf-8"))
        assert isinstance(key_data, int)
    else:  # string
        key_str = first_msg.key().decode("utf-8")
        assert key_str.isdigit()

    if value_serializer == "json":
        value_data = json.loads(first_msg.value().decode("utf-8"))
        assert "id" in value_data
        assert "message" in value_data
    else:  # string
        value_str = first_msg.value().decode("utf-8")
        assert "id" in value_str
        assert "message" in value_str


def test_write_kafka_multiple_blocks(
    bootstrap_server, kafka_consumer, ray_start_regular_shared
):
    """Test writing dataset with multiple blocks."""
    topic = "test-write-multiple-blocks"

    # Create dataset and repartition to ensure multiple blocks
    ds = ray.data.range(200).repartition(5)

    # Write to Kafka
    ds.write_kafka(topic=topic, bootstrap_servers=bootstrap_server)

    # Consume and verify
    messages = consume_messages(kafka_consumer, topic, expected_count=200)
    assert len(messages) == 200

    # Verify all ids are present
    ids = set()
    for msg in messages:
        value = json.loads(msg.value().decode("utf-8"))
        ids.add(value["id"])

    assert len(ids) == 200
    assert ids == set(range(200))


def test_write_kafka_empty_dataset(
    bootstrap_server, kafka_consumer, ray_start_regular_shared
):
    """Test writing an empty dataset."""
    topic = "test-write-empty"

    # Create empty dataset
    ds = ray.data.from_items([])

    # Write to Kafka (should succeed without errors)
    ds.write_kafka(topic=topic, bootstrap_servers=bootstrap_server)

    # Try to consume (should get no messages)
    messages = consume_messages(kafka_consumer, topic, expected_count=0, timeout=3)
    assert len(messages) == 0


def test_write_kafka_with_producer_config(
    bootstrap_server, kafka_consumer, ray_start_regular_shared
):
    """Test writing with custom producer configuration."""
    topic = "test-write-producer-config"

    # Create dataset
    ds = ray.data.range(30)

    # Write with custom producer config (confluent-kafka/librdkafka format)
    ds.write_kafka(
        topic=topic,
        bootstrap_servers=bootstrap_server,
        producer_config={
            "acks": "all",
            "retries": 3,
            "max.in.flight.requests.per.connection": 1,
        },
    )

    # Consume and verify
    messages = consume_messages(kafka_consumer, topic, expected_count=30)
    assert len(messages) == 30


def test_write_kafka_with_complex_data(
    bootstrap_server, kafka_consumer, ray_start_regular_shared
):
    """Test writing complex nested data structures."""
    topic = "test-write-complex"

    # Create dataset with nested structures
    data = [
        {
            "id": i,
            "user": {"name": f"user-{i}", "email": f"user{i}@example.com"},
            "tags": [f"tag{j}" for j in range(3)],
            "metadata": {"created": "2024-01-01", "score": i * 1.5},
        }
        for i in range(15)
    ]
    ds = ray.data.from_items(data)

    # Write to Kafka
    ds.write_kafka(topic=topic, bootstrap_servers=bootstrap_server)

    # Consume and verify
    messages = consume_messages(kafka_consumer, topic, expected_count=15)
    assert len(messages) == 15

    # Verify nested structure is preserved
    first_value = json.loads(messages[0].value().decode("utf-8"))
    assert "user" in first_value
    assert "name" in first_value["user"]
    assert "tags" in first_value
    assert isinstance(first_value["tags"], list)
    assert "metadata" in first_value


def test_write_kafka_invalid_bootstrap_server(ray_start_regular_shared):
    """Test error handling with invalid bootstrap server."""
    topic = "test-invalid-server"

    ds = ray.data.range(10)

    # Use a short message.timeout.ms so librdkafka gives up quickly
    # instead of waiting the full 30s flush timeout.
    with pytest.raises(Exception):
        ds.write_kafka(
            topic=topic,
            bootstrap_servers="invalid-server:9999",
            producer_config={"message.timeout.ms": 3000},
        )


def test_write_kafka_dataset_with_nulls(
    bootstrap_server, kafka_consumer, ray_start_regular_shared
):
    """Test writing dataset with null/None values."""
    topic = "test-write-nulls"

    # Create dataset with None values
    data = [{"id": i, "value": f"val-{i}" if i % 2 == 0 else None} for i in range(20)]
    ds = ray.data.from_items(data)

    # Write to Kafka
    ds.write_kafka(topic=topic, bootstrap_servers=bootstrap_server)

    # Consume and verify
    messages = consume_messages(kafka_consumer, topic, expected_count=20)
    assert len(messages) == 20

    # Verify None values are serialized
    for msg in messages:
        value = json.loads(msg.value().decode("utf-8"))
        assert "id" in value
        # value["value"] should be either a string or null


@pytest.mark.parametrize(
    "start_offset,expected_error",
    [
        (
            {"my-topic": {0: "latest"}},
            r"start_offset\['my-topic'\]\[0\] cannot be 'latest'",
        ),
        (
            {"my-topic": {"not-an-int": 100}},
            r"start_offset\['my-topic'\] keys must be integers",
        ),
        (
            {"my-topic": "not-a-dict"},
            r"start_offset\['my-topic'\] must be a dict",
        ),
    ],
)
def test_per_partition_start_offset_invalid_values(start_offset, expected_error):
    """Per-partition start_offset with disallowed values raises ValueError at init."""
    with pytest.raises(ValueError, match=expected_error):
        ray.data.read_kafka(
            topics="my-topic",
            bootstrap_servers="localhost:9092",
            start_offset=start_offset,
        )


@pytest.mark.parametrize(
    "end_offset,expected_error",
    [
        (
            {"my-topic": {0: "earliest"}},
            r"end_offset\['my-topic'\]\[0\] cannot be 'earliest'",
        ),
        (
            {"my-topic": {"not-an-int": 100}},
            r"end_offset\['my-topic'\] keys must be integers",
        ),
        (
            {"my-topic": "not-a-dict"},
            r"end_offset\['my-topic'\] must be a dict",
        ),
    ],
)
def test_per_partition_end_offset_invalid_values(end_offset, expected_error):
    """Per-partition end_offset with disallowed values raises ValueError at init."""
    with pytest.raises(ValueError, match=expected_error):
        ray.data.read_kafka(
            topics="my-topic",
            bootstrap_servers="localhost:9092",
            end_offset=end_offset,
        )


def test_per_partition_start_offset_non_existent_partition(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Per-partition dict referencing a non-existent partition raises ValueError."""
    topic = "test-per-partition-bad-partition"

    for i in range(10):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.produce(topic, value=message)
    kafka_producer.flush()
    time.sleep(0.3)

    with pytest.raises(
        ValueError,
        match=r"start_offset references partition 99 in topic",
    ):
        ds = ray.data.read_kafka(
            topics=[topic],
            bootstrap_servers=[bootstrap_server],
            start_offset={topic: {99: 0}},
        )
        ds.take_all()


def test_per_partition_start_offset_specific_offsets(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Per-partition start_offset reads correct slice from each partition."""
    from confluent_kafka.admin import AdminClient, NewTopic

    topic = "test-per-partition-start-offset"

    admin_client = AdminClient({"bootstrap.servers": bootstrap_server})
    topic_config = NewTopic(topic, num_partitions=2, replication_factor=1)
    admin_client.create_topics([topic_config])
    time.sleep(2)  # Wait for topic creation

    # Send 50 messages to partition 0 and 50 to partition 1
    for i in range(50):
        kafka_producer.produce(topic, value={"id": i}, partition=0)
        kafka_producer.produce(topic, value={"id": i}, partition=1)
    kafka_producer.flush()
    time.sleep(0.3)

    # Read partition 0 from offset 20, partition 1 from offset 40
    # Expected: 30 messages from partition 0 + 10 messages from partition 1 = 40
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset={topic: {0: 20, 1: 40}},
        end_offset="latest",
    )

    records = ds.take_all()
    assert len(records) == 40


def test_per_partition_end_offset_specific_offsets(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Per-partition end_offset reads correct slice from each partition."""
    from confluent_kafka.admin import AdminClient, NewTopic

    topic = "test-per-partition-end-offset"

    admin_client = AdminClient({"bootstrap.servers": bootstrap_server})
    topic_config = NewTopic(topic, num_partitions=2, replication_factor=1)
    admin_client.create_topics([topic_config])
    time.sleep(2)  # Wait for topic creation

    # Send 50 messages to partition 0 and 50 to partition 1
    for i in range(50):
        kafka_producer.produce(topic, value={"id": i}, partition=0)
        kafka_producer.produce(topic, value={"id": i}, partition=1)
    kafka_producer.flush()
    time.sleep(0.3)

    # End partition 0 at offset 30, partition 1 at offset 20
    # Expected: 30 messages from partition 0 + 20 messages from partition 1 = 50
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset="earliest",
        end_offset={topic: {0: 30, 1: 20}},
    )

    records = ds.take_all()
    assert len(records) == 50


def test_per_partition_start_offset_fallback_to_earliest(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Partitions not listed in per-partition dict fall back to 'earliest'."""
    from confluent_kafka.admin import AdminClient, NewTopic

    topic = "test-per-partition-fallback"

    admin_client = AdminClient({"bootstrap.servers": bootstrap_server})
    topic_config = NewTopic(topic, num_partitions=2, replication_factor=1)
    admin_client.create_topics([topic_config])
    time.sleep(2)  # Wait for topic creation

    # Send 50 messages to each partition
    for i in range(50):
        kafka_producer.produce(topic, value={"id": i}, partition=0)
        kafka_producer.produce(topic, value={"id": i}, partition=1)
    kafka_producer.flush()
    time.sleep(0.3)

    # Only specify offset for partition 0; partition 1 should fall back to earliest (0)
    # Expected: 30 messages from partition 0 + 50 messages from partition 1 = 80
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset={topic: {0: 20}},
        end_offset="latest",
    )

    records = ds.take_all()
    assert len(records) == 80


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
