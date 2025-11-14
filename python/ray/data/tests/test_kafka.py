import json
import time

import pytest

import ray

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


def test_read_kafka_basic(bootstrap_server, kafka_producer, ray_start_regular_shared):
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


@pytest.mark.parametrize(
    "total_messages,start_offset,end_offset,expected_count,test_id",
    [
        (100, 20, 80, 60, "both-set"),
        (100, 50, None, 50, "start-offset-only"),
        (100, None, 50, 50, "end-offset-only"),
        (100, None, None, 100, "both-none"),
        (100, "earliest", 30, 30, "earliest-start-offset-number-end-offset"),
        (100, 50, "latest", 50, "number-start-offset-number-end-offset"),
        (100, "20", "80", 60, "number-start-offset-string-number-end-offset-string"),
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

    # Send test messages
    for i in range(total_messages):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.send(topic, value=message)
    kafka_producer.flush()
    time.sleep(1)

    # Read with specified offsets
    kwargs = {
        "topics": [topic],
        "bootstrap_servers": [bootstrap_server],
    }
    if start_offset is not None:
        kwargs["start_offset"] = start_offset
    if end_offset is not None:
        kwargs["end_offset"] = end_offset

    ds = ray.data.read_kafka(**kwargs)

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


def test_read_kafka_offset_exceeds_available_messages(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    import time

    topic = "test-offset-timeout"

    # Send only 50 messages
    for i in range(50):
        message = {"id": i, "value": f"message-{i}"}
        kafka_producer.send(topic, value=message)
    kafka_producer.flush()
    time.sleep(1)

    # Try to read up to offset 200 (way beyond available messages)
    # This should timeout and only return the 50 available messages

    start_time = time.time()
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset=0,
        end_offset=200,
        timeout_ms=5000,  # 5 second timeout
    )

    records = ds.take_all()

    elapsed_time = time.time() - start_time

    # Should get all 50 available messages
    assert len(records) == 50

    # Should have waited for timeout (at least 4.5 seconds to account for some variance)
    assert elapsed_time >= 4.5, f"Expected timeout wait, but only took {elapsed_time}s"


def test_read_kafka_invalid_topic(bootstrap_server, ray_start_regular_shared):
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
        [80, 20, "start_offset must be less than end_offset"],
    ],
)
def test_read_kafka_invalid_offsets(
    bootstrap_server, ray_start_regular_shared, test_case
):
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
