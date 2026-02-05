import json
import time

import pytest

import ray
from ray.data._internal.datasource.kafka_datasink import KafkaDatasink

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


# Unit Tests


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


# Integration Tests (require Kafka container)


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
