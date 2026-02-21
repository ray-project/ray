import json
import time
from datetime import datetime

import pytest

import ray
from ray.data._internal.datasource.kafka_datasink import KafkaDatasink
from ray.data._internal.datasource.kafka_datasource import (
    KafkaAuthConfig,
    _add_authentication_to_config,
    _build_consumer_config_for_discovery,
    _build_consumer_config_for_read,
    _datetime_to_ms,
)

pytest.importorskip("kafka")


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


@pytest.fixture(scope="session")
def kafka_producer(bootstrap_server):
    from kafka import KafkaProducer

    print(f"Creating shared Kafka producer for {bootstrap_server}")
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    yield producer
    print("Closing shared Kafka producer")
    producer.close()


@pytest.fixture(scope="session")
def kafka_consumer(bootstrap_server):
    from kafka import KafkaConsumer

    print(f"Creating shared Kafka consumer for {bootstrap_server}")
    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_server],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: v,  # Keep as bytes for flexibility
        key_deserializer=lambda k: k,  # Keep as bytes for flexibility
        consumer_timeout_ms=5000,
    )
    yield consumer
    print("Closing shared Kafka consumer")
    consumer.close()


def consume_messages(consumer, topic, expected_count, timeout=10):
    """Helper function to consume messages from a topic."""
    from kafka import TopicPartition

    # Subscribe and get partitions
    consumer.subscribe([topic])
    time.sleep(1)  # Wait for subscription

    # Get all partitions for the topic
    partitions = consumer.partitions_for_topic(topic)
    if partitions is None:
        time.sleep(2)  # Wait a bit more for topic to be created
        partitions = consumer.partitions_for_topic(topic)

    if partitions:
        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(topic_partitions)
        consumer.seek_to_beginning(*topic_partitions)

    messages = []
    start_time = time.time()

    while len(messages) < expected_count and (time.time() - start_time) < timeout:
        records = consumer.poll(timeout_ms=1000)
        for topic_partition, records_list in records.items():
            messages.extend(records_list)

    return messages


def test_add_authentication_to_config():
    """Test authentication config passthrough with all kafka-python auth parameters."""
    # Test empty authentication
    config = {}
    _add_authentication_to_config(config, None)
    assert config == {}

    _add_authentication_to_config(config, {})
    assert config == {}

    # Test all authentication parameters at once
    config = {}
    token_provider = object()
    ssl_context = object()  # Mock SSL context

    kafka_auth_config = KafkaAuthConfig(
        # Security protocol
        security_protocol="SASL_SSL",
        # SASL configuration
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username="testuser",
        sasl_plain_password="testpass",
        sasl_kerberos_name="kafka/hostname@REALM",
        sasl_kerberos_service_name="kafka",
        sasl_kerberos_domain_name="example.com",
        sasl_oauth_token_provider=token_provider,
        # SSL configuration
        ssl_context=ssl_context,
        ssl_check_hostname=False,
        ssl_cafile="/path/to/ca.pem",
        ssl_certfile="/path/to/cert.pem",
        ssl_keyfile="/path/to/key.pem",
        ssl_password="keypassword",
        ssl_ciphers="HIGH:!aNULL",
        ssl_crlfile="/path/to/crl.pem",
    )

    _add_authentication_to_config(config, kafka_auth_config)

    # Verify all parameters are passed through correctly
    assert config["security_protocol"] == "SASL_SSL"
    assert config["sasl_mechanism"] == "SCRAM-SHA-256"
    assert config["sasl_plain_username"] == "testuser"
    assert config["sasl_plain_password"] == "testpass"
    assert config["sasl_kerberos_name"] == "kafka/hostname@REALM"
    assert config["sasl_kerberos_service_name"] == "kafka"
    assert config["sasl_kerberos_domain_name"] == "example.com"
    assert config["sasl_oauth_token_provider"] == token_provider
    assert config["ssl_context"] == ssl_context
    assert config["ssl_check_hostname"] is False
    assert config["ssl_cafile"] == "/path/to/ca.pem"
    assert config["ssl_certfile"] == "/path/to/cert.pem"
    assert config["ssl_keyfile"] == "/path/to/key.pem"
    assert config["ssl_password"] == "keypassword"
    assert config["ssl_ciphers"] == "HIGH:!aNULL"
    assert config["ssl_crlfile"] == "/path/to/crl.pem"


def test_build_consumer_config_for_discovery():
    bootstrap_servers = ["localhost:9092", "localhost:9093"]

    # Test without authentication
    config = _build_consumer_config_for_discovery(bootstrap_servers, {})
    assert config["bootstrap_servers"] == bootstrap_servers
    assert config["enable_auto_commit"] is False
    assert config["consumer_timeout_ms"] == 1000
    assert "group_id" not in config

    # Test with authentication
    kafka_auth_config = KafkaAuthConfig(
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username="admin",
        sasl_plain_password="secret",
    )
    config = _build_consumer_config_for_discovery(bootstrap_servers, kafka_auth_config)
    assert config["security_protocol"] == "SASL_SSL"
    assert config["sasl_mechanism"] == "SCRAM-SHA-256"
    assert config["sasl_plain_username"] == "admin"
    assert config["sasl_plain_password"] == "secret"


def test_build_consumer_config_for_read():
    """Test read config builder."""
    bootstrap_servers = ["localhost:9092"]

    # Test basic config
    config = _build_consumer_config_for_read(bootstrap_servers, {})
    assert config["bootstrap_servers"] == bootstrap_servers
    assert config["enable_auto_commit"] is False
    assert config["value_deserializer"] is not None
    assert config["key_deserializer"] is not None

    # Test with authentication
    kafka_auth_config = KafkaAuthConfig(
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="user",
        sasl_plain_password="pass",
    )
    config_with_auth = _build_consumer_config_for_read(
        bootstrap_servers, kafka_auth_config
    )
    assert config_with_auth["security_protocol"] == "SASL_SSL"
    assert config_with_auth["sasl_mechanism"] == "PLAIN"
    assert config_with_auth["sasl_plain_username"] == "user"
    assert config_with_auth["sasl_plain_password"] == "pass"


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
        kafka_producer.send(topic, value=message, key=f"key-{i}")
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

    # Verify data types: key is string, value is binary
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
        kafka_producer.send(topic, value=message)
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
    from kafka.admin import KafkaAdminClient, NewTopic

    topic = "test-multi-partition"

    # Create topic with 3 partitions
    admin_client = KafkaAdminClient(bootstrap_servers=[bootstrap_server])
    topic_config = NewTopic(name=topic, num_partitions=3, replication_factor=1)
    admin_client.create_topics([topic_config])
    admin_client.close()

    time.sleep(2)  # Wait for topic creation

    # Send messages to different partitions
    for i in range(150):
        message = {"id": i, "value": f"message-{i}"}
        partition = i % 3
        kafka_producer.send(topic, value=message, partition=partition)
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
        kafka_producer.send(topic1, value=message)

    # Send messages to topic2
    for i in range(30):
        message = {"id": i, "value": f"topic2-message-{i}"}
        kafka_producer.send(topic2, value=message)

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
        kafka_producer.send(topic, value=message, headers=headers)
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
    "start_offset,end_offset,expected_count, test_id",
    [
        (150, 200, 0, "start-offset-exceeds-available-messages"),
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
    import time

    topic = f"test-offset-timeout-{test_id}"

    for i in range(100):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.send(topic, value=message)
    kafka_producer.flush()
    time.sleep(0.3)

    # Try to read up to offset 200 (way beyond available messages)
    # This should timeout and only return the 50 available messages

    start_time = time.time()
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset=start_offset,
        end_offset=end_offset,
        timeout_ms=3000,  # 3 second timeout
    )

    records = ds.take_all()

    elapsed_time = time.time() - start_time

    # Should get all 50 available messages
    assert len(records) == expected_count

    assert elapsed_time >= 3, f"Expected timeout wait, but only took {elapsed_time}s"


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
        kafka_producer.send(topic, value=message)
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

    msg_ts = _datetime_to_ms(datetime(2025, 1, 15))
    time_before = datetime(2025, 1, 1)
    time_after = datetime(2025, 2, 1)

    for i in range(3):
        kafka_producer.send(topic, value={"id": i}, timestamp_ms=msg_ts)
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

    kafka_producer.send(topic, value={"batch": 1, "id": 0}, timestamp_ms=batch1_ts)
    kafka_producer.send(topic, value={"batch": 1, "id": 1}, timestamp_ms=batch1_ts)
    kafka_producer.send(topic, value={"batch": 2, "id": 0}, timestamp_ms=batch2_ts)
    kafka_producer.send(topic, value={"batch": 2, "id": 1}, timestamp_ms=batch2_ts)
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

    kafka_producer.send(topic, value={"id": 0})
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

    kafka_producer.send(topic, value={"id": 0})
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


def test_flush_futures_all_success():
    """Test _flush_futures returns (0, None) when all futures succeed."""
    from unittest.mock import MagicMock

    sink = KafkaDatasink(topic="test", bootstrap_servers="localhost:9092")
    mock_producer = MagicMock()

    futures = [MagicMock() for _ in range(5)]
    for f in futures:
        f.get.return_value = MagicMock()  # Simulate successful metadata

    failed, first_exc = sink._flush_futures(futures, mock_producer)

    assert failed == 0
    assert first_exc is None
    mock_producer.flush.assert_called_once_with(timeout=30.0)
    for f in futures:
        f.get.assert_called_once_with(timeout=0)


def test_flush_futures_preserves_first_exception():
    """Test _flush_futures captures the first exception, not the last."""
    from unittest.mock import MagicMock

    sink = KafkaDatasink(topic="test", bootstrap_servers="localhost:9092")
    mock_producer = MagicMock()

    error_a = Exception("Error A")
    error_b = Exception("Error B")

    futures = [MagicMock(), MagicMock(), MagicMock()]
    futures[0].get.side_effect = error_a
    futures[1].get.side_effect = error_b
    futures[2].get.return_value = MagicMock()  # Success

    failed, first_exc = sink._flush_futures(futures, mock_producer)

    assert failed == 2
    assert first_exc is error_a  # First exception, not error_b


def test_write_raises_with_chained_exception():
    """Test write() chains the first Kafka error into the RuntimeError."""
    from unittest.mock import MagicMock, patch

    sink = KafkaDatasink(topic="test-topic", bootstrap_servers="localhost:9092")

    # Create a mock future whose .get() raises
    kafka_error = Exception("TopicAuthorizationFailedError")
    mock_future = MagicMock()
    mock_future.get.side_effect = kafka_error

    # Mock KafkaProducer so write() doesn't need a real broker
    mock_producer = MagicMock()
    mock_producer.send.return_value = mock_future

    # Build a minimal block that yields one row
    mock_block = MagicMock()
    mock_accessor = MagicMock()
    mock_accessor.iter_rows.return_value = [{"id": 1}]

    with (
        patch(
            "ray.data._internal.datasource.kafka_datasink.KafkaProducer",
            return_value=mock_producer,
        ),
        patch(
            "ray.data._internal.datasource.kafka_datasink.BlockAccessor.for_block",
            return_value=mock_accessor,
        ),
    ):
        with pytest.raises(
            RuntimeError, match="Failed to write 1 out of 1"
        ) as exc_info:
            sink.write([mock_block], ctx=MagicMock())

        # Verify exception chaining: __cause__ should be the original Kafka error
        assert exc_info.value.__cause__ is kafka_error


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
    assert first_msg.topic == topic
    assert first_msg.key is None  # No key field specified

    # Verify value is JSON encoded
    value = json.loads(first_msg.value.decode("utf-8"))
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
        assert msg.key is not None
        key_str = msg.key.decode("utf-8")
        assert key_str.isdigit()

        # Verify value
        value = json.loads(msg.value.decode("utf-8"))
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
        key_data = json.loads(first_msg.key.decode("utf-8"))
        assert isinstance(key_data, int)
    else:  # string
        key_str = first_msg.key.decode("utf-8")
        assert key_str.isdigit()

    if value_serializer == "json":
        value_data = json.loads(first_msg.value.decode("utf-8"))
        assert "id" in value_data
        assert "message" in value_data
    else:  # string
        value_str = first_msg.value.decode("utf-8")
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
        value = json.loads(msg.value.decode("utf-8"))
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

    # Write with custom producer config
    ds.write_kafka(
        topic=topic,
        bootstrap_servers=bootstrap_server,
        producer_config={
            "acks": "all",
            "retries": 3,
            "max_in_flight_requests_per_connection": 1,
        },
    )

    # Consume and verify
    messages = consume_messages(kafka_consumer, topic, expected_count=30)
    assert len(messages) == 30


def test_write_kafka_with_delivery_callback(
    bootstrap_server, kafka_consumer, ray_start_regular_shared
):
    """Test writing with a delivery callback."""
    topic = "test-write-callback"

    # Track callback invocations
    callback_results = {"success": 0, "error": 0}

    def delivery_callback(metadata=None, exception=None):
        if exception:
            callback_results["error"] += 1
        else:
            callback_results["success"] += 1

    # Create dataset
    ds = ray.data.range(25)

    # Write with callback
    ds.write_kafka(
        topic=topic,
        bootstrap_servers=bootstrap_server,
        delivery_callback=delivery_callback,
    )

    # Consume and verify
    messages = consume_messages(kafka_consumer, topic, expected_count=25)
    assert len(messages) == 25

    # Note: Callbacks are invoked asynchronously, so we may or may not
    # have callback results by the time we get here. We just verify
    # that no exceptions were raised during write.


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
    first_value = json.loads(messages[0].value.decode("utf-8"))
    assert "user" in first_value
    assert "name" in first_value["user"]
    assert "tags" in first_value
    assert isinstance(first_value["tags"], list)
    assert "metadata" in first_value


def test_write_kafka_invalid_bootstrap_server(ray_start_regular_shared):
    """Test error handling with invalid bootstrap server."""
    topic = "test-invalid-server"

    ds = ray.data.range(10)

    # Should raise an error when trying to connect
    with pytest.raises(Exception):  # Will raise a Kafka-related error
        ds.write_kafka(topic=topic, bootstrap_servers="invalid-server:9999")


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
        value = json.loads(msg.value.decode("utf-8"))
        assert "id" in value
        # value["value"] should be either a string or null


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
