import json
import time
from datetime import datetime

import pytest

import ray
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


def test_kafka_datasource_init_per_partition_valid():
    """Valid per-partition start_offset dicts must pass __init__ without error."""
    from ray.data._internal.datasource.kafka_datasource import KafkaDatasource

    # Mix of int, "earliest", and datetime per-partition offsets
    KafkaDatasource(
        topics=["my_topic"],
        bootstrap_servers="localhost:9092",
        start_offset={
            "my_topic": {0: 1500, 1: "earliest", 2: datetime(2025, 1, 1)}
        },
    )

    # Empty dict (no per-partition overrides) is also valid
    KafkaDatasource(
        topics=["my_topic"],
        bootstrap_servers="localhost:9092",
        start_offset={},
    )


@pytest.mark.parametrize(
    "start_offset,expected_error",
    [
        (
            {42: {0: 100}},
            "Per-partition start_offset dict keys must be topic name strings",
        ),
        (
            {"other_topic": {0: 100}},
            "which is not in the topics being read",
        ),
        (
            {"my_topic": 100},
            "Per-partition start_offset values must be dicts",
        ),
        (
            {"my_topic": {"0": 100}},
            "Partition keys in per-partition start_offset must be ints",
        ),
        (
            {"my_topic": {True: 100}},
            "Partition keys in per-partition start_offset must be ints",
        ),
        (
            {"my_topic": {0: True}},
            "Per-partition start_offset values must be int, 'earliest', or datetime",
        ),
        (
            {"my_topic": {0: 3.14}},
            "Per-partition start_offset values must be int, 'earliest', or datetime",
        ),
        (
            {"my_topic": {0: "latest"}},
            "Per-partition start_offset string values must be 'earliest'",
        ),
    ],
)
def test_kafka_datasource_init_per_partition_invalid(start_offset, expected_error):
    """Invalid per-partition start_offset dicts must raise ValueError at init time."""
    from ray.data._internal.datasource.kafka_datasource import KafkaDatasource

    with pytest.raises(ValueError, match=expected_error):
        KafkaDatasource(
            topics=["my_topic"],
            bootstrap_servers="localhost:9092",
            start_offset=start_offset,
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


def test_read_kafka_per_partition_offsets(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Per-partition start offsets must be applied per-partition; omitted
    partitions must fall back to 'earliest'."""
    from kafka.admin import KafkaAdminClient, NewTopic

    topic = "test-per-partition-offsets"
    admin_client = KafkaAdminClient(bootstrap_servers=[bootstrap_server])
    admin_client.create_topics(
        [NewTopic(name=topic, num_partitions=3, replication_factor=1)]
    )
    admin_client.close()
    time.sleep(2)

    # 90 messages round-robined across 3 partitions → 30 messages per partition
    # at offsets 0-29 in each partition.
    for i in range(90):
        kafka_producer.send(topic, value={"id": i}, partition=i % 3)
    kafka_producer.flush()
    time.sleep(0.3)

    # Partition 0: start at offset 10 → reads offsets 10-29 = 20 messages.
    # Partitions 1 and 2: omitted from dict → fall back to "earliest" = 30 messages each.
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset={topic: {0: 10}},
        end_offset="latest",
    )
    records = ds.take_all()
    assert len(records) == 80  # 20 + 30 + 30

    # Partition 0 records must all start at or after offset 10.
    p0_records = [r for r in records if r["partition"] == 0]
    assert len(p0_records) == 20
    assert all(r["offset"] >= 10 for r in p0_records)


def test_read_kafka_per_partition_earliest_string(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Explicit 'earliest' per-partition offset must read from the beginning."""
    from kafka.admin import KafkaAdminClient, NewTopic

    topic = "test-per-partition-earliest"
    admin_client = KafkaAdminClient(bootstrap_servers=[bootstrap_server])
    admin_client.create_topics(
        [NewTopic(name=topic, num_partitions=2, replication_factor=1)]
    )
    admin_client.close()
    time.sleep(2)

    for i in range(40):
        kafka_producer.send(topic, value={"id": i}, partition=i % 2)
    kafka_producer.flush()
    time.sleep(0.3)

    # Both partitions listed explicitly with "earliest" — should return all 40 messages.
    ds = ray.data.read_kafka(
        topics=[topic],
        bootstrap_servers=[bootstrap_server],
        start_offset={topic: {0: "earliest", 1: "earliest"}},
        end_offset="latest",
    )
    records = ds.take_all()
    assert len(records) == 40


def test_read_kafka_per_partition_nonexistent_partition(
    bootstrap_server, kafka_producer, ray_start_regular_shared
):
    """Specifying a partition that does not exist on the broker must raise ValueError
    with a clear message before any read tasks are launched."""
    from kafka.admin import KafkaAdminClient, NewTopic

    topic = "test-invalid-partition-id"
    admin_client = KafkaAdminClient(bootstrap_servers=[bootstrap_server])
    admin_client.create_topics(
        [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
    )
    admin_client.close()
    time.sleep(2)

    kafka_producer.send(topic, value={"id": 0})
    kafka_producer.flush()
    time.sleep(0.3)

    # Partition 99 does not exist — only partition 0 does.
    with pytest.raises(ValueError, match="does not exist on the broker"):
        ds = ray.data.read_kafka(
            topics=[topic],
            bootstrap_servers=[bootstrap_server],
            start_offset={topic: {99: 0}},
        )
        ds.take_all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
