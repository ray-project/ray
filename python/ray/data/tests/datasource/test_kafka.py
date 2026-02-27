import json
import time
from datetime import datetime

import pytest

import ray
from ray.data._internal.datasource.kafka_datasource import (
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


def test_build_consumer_config_for_read():
    """Test read config builder."""
    bootstrap_servers = ["localhost:9092"]

    # Test basic config
    config = _build_consumer_config_for_read(bootstrap_servers, None)
    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["enable.auto.commit"] is False
    assert "group.id" in config

    # Test with authentication
    user_conf = {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "user",
        "sasl.password": "pass",
    }
    config_with_auth = _build_consumer_config_for_read(bootstrap_servers, user_conf)
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
    config = _build_consumer_config_for_read(bootstrap_servers, extra)
    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["ssl.endpoint.identification.algorithm"] == "none"
    assert config["group.id"] == "custom-group"
    assert config["enable.auto.commit"] is True

    # Attempt to override bootstrap.servers should be ignored
    override = {"bootstrap.servers": "override:9092"}
    config2 = _build_consumer_config_for_read(bootstrap_servers, override)
    assert config2["bootstrap.servers"] == "localhost:9092"


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
        kafka_producer.produce(topic, value=message)
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

    msg_ts = _datetime_to_ms(datetime(2025, 1, 15))
    time_before = datetime(2025, 1, 1)
    time_after = datetime(2025, 2, 1)

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
