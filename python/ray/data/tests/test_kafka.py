import json
import time

import pytest

import ray

pytest.importorskip("kafka")


@pytest.fixture(scope="session")
def kafka_container():
    """Session-scoped fixture that provides a Kafka container for all tests."""
    from testcontainers.kafka import KafkaContainer

    print("\nStarting Kafka container (shared across all tests)...")
    with KafkaContainer() as kafka:
        bootstrap_server = kafka.get_bootstrap_server()
        print(f"Kafka container started at {bootstrap_server}")
        yield kafka
        print("\nShutting down Kafka container...")


@pytest.fixture(scope="session")
def bootstrap_server(kafka_container):
    """Convenience fixture to get the Kafka bootstrap server address."""
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def kafka_producer(bootstrap_server):
    """Session-scoped fixture that provides a shared Kafka producer."""
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


def test_read_kafka_basic(bootstrap_server, kafka_producer, ray_start_regular_shared):
    """Test basic Kafka reading functionality."""
    topic = "test-basic"

    # Send test messages
    for i in range(100):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.send(topic, value=message, key=f"key-{i}")
    kafka_producer.flush()

    # Wait a bit for messages to be committed
    time.sleep(1)

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


def test_read_kafka_with_start_offset(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Test reading from a specific start offset."""
    topic = "test-start-offset"

    # Send test messages
    for i in range(100):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.send(topic, value=message)
    kafka_producer.flush()
    time.sleep(1)

    # Read from offset 50
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset=50,
    )

    records = ds.take_all()
    assert len(records) == 50  # Should read from 50 to 100


def test_read_kafka_with_end_offset(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Test reading up to a specific end offset."""
    topic = "test-end-offset"

    # Send test messages
    for i in range(100):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.send(topic, value=message)
    kafka_producer.flush()
    time.sleep(1)

    # Read from 0 to 50
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset=0,
        end_offset=50,
    )

    records = ds.take_all()
    assert len(records) == 50


def test_read_kafka_with_offset_range(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Test reading a specific offset range."""
    topic = "test-offset-range"

    # Send test messages
    for i in range(100):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.send(topic, value=message)
    kafka_producer.flush()
    time.sleep(1)

    # Read from 20 to 80
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset=20,
        end_offset=80,
    )

    records = ds.take_all()
    assert len(records) == 60


def test_read_kafka_earliest_offset(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Test reading from earliest offset."""
    topic = "test-earliest"

    # Send test messages
    for i in range(50):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.send(topic, value=message)
    kafka_producer.flush()
    time.sleep(1)

    # Read from earliest
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset="earliest",
    )

    records = ds.take_all()
    assert len(records) == 50


def test_read_kafka_multiple_partitions(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Test reading from multiple partitions."""
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
    time.sleep(1)

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
    """Test reading from multiple topics."""
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
    time.sleep(1)

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
    """Test reading messages with headers."""
    topic = "test-headers"

    # Send messages with headers
    for i in range(10):
        message = {"id": i, "value": f"message-{i}"}
        headers = [
            ("header1", b"value1"),
            ("header2", f"value-{i}".encode("utf-8")),
        ]
        kafka_producer.send(topic, value=message, headers=headers)
    kafka_producer.flush()
    time.sleep(1)

    # Read messages
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
    )

    records = ds.take_all()
    assert len(records) == 10

    # Verify headers
    first_record = records[0]
    assert "headers" in first_record
    assert first_record["headers"]["header1"] == "value1"


def test_read_kafka_empty_topic(bootstrap_server, ray_start_regular_shared):
    """Test reading from an empty topic."""
    from kafka.admin import KafkaAdminClient, NewTopic

    topic = "test-empty"

    # Create empty topic
    admin_client = KafkaAdminClient(bootstrap_servers=[bootstrap_server])
    topic_config = NewTopic(name=topic, num_partitions=1, replication_factor=1)
    admin_client.create_topics([topic_config])
    admin_client.close()

    time.sleep(2)

    # Read from empty topic
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
    )

    records = ds.take_all()
    assert len(records) == 0


def test_read_kafka_invalid_topic(bootstrap_server, ray_start_regular_shared):
    """Test reading from non-existent topic."""
    with pytest.raises(ValueError, match="has no partitions or doesn't exist"):
        ds = ray.data.read_kafka(
            topics=["non-existent-topic"],
            bootstrap_servers=[bootstrap_server],
        )
        ds.take_all()


@pytest.mark.parametrize(
    "test_case",
    [
        [0, "earliest", "end_offset cannot be 'earliest'"],
        ["latest", 1000, "start_offset cannot be 'latest'"],
    ],
)
def test_read_kafka_latest_offset_not_allowed_for_end(
    bootstrap_server, ray_start_regular_shared, test_case
):
    """Test that 'latest' is not allowed for end_offset."""
    with pytest.raises(ValueError, match=test_case[2]):
        ds = ray.data.read_kafka(
            topics=["test-topic"],
            bootstrap_servers=[bootstrap_server],
            start_offset=test_case[0],
            end_offset=test_case[1],
        )
        ds.take_all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
