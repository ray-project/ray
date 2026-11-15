import sys

import pyarrow as pa
import pytest

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def test_arrow_flight_transport_basic(ray_start_regular):
    """Test basic send/receive of an Arrow table between two actors."""

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="arrow_flight")
        def produce(self):
            return pa.table({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})

        def consume(self, data):
            return data.column("a").to_pylist()

    actors = [Actor.remote() for _ in range(2)]
    ref = actors[0].produce.remote()
    result = actors[1].consume.remote(ref)
    assert ray.get(result) == [1, 2, 3]


def test_arrow_flight_transport_multiple_tables(ray_start_regular):
    """Test sending a list of multiple Arrow tables."""

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="arrow_flight")
        def produce(self):
            return [
                pa.table({"x": [1, 2]}),
                pa.table({"y": [3, 4, 5]}),
            ]

        def total_rows(self, data):
            return sum(t.num_rows for t in data)

    actors = [Actor.remote() for _ in range(2)]
    ref = actors[0].produce.remote()
    result = actors[1].total_rows.remote(ref)
    assert ray.get(result) == 5


def test_arrow_flight_transport_large_table(ray_start_regular):
    """Test sending a large Arrow table."""

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="arrow_flight")
        def produce_large(self):
            return pa.table({"val": list(range(1_000_000))})

        def verify(self, data):
            col = data.column("val")
            return data.num_rows == 1_000_000 and col[0].as_py() == 0

    actors = [Actor.remote() for _ in range(2)]
    ref = actors[0].produce_large.remote()
    result = actors[1].verify.remote(ref)
    assert ray.get(result)


def test_arrow_flight_transport_concurrent_receivers(ray_start_regular):
    """Test multiple receivers fetching the same table."""

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="arrow_flight")
        def produce(self):
            return pa.table({"a": [10, 20, 30]})

        def row_count(self, data):
            return data.num_rows

    producer = Actor.remote()
    consumers = [Actor.remote() for _ in range(3)]

    ref = producer.produce.remote()
    results = ray.get([c.row_count.remote(ref) for c in consumers])
    assert all(r == 3 for r in results)


def test_arrow_flight_transport_schema_preservation(ray_start_regular):
    """Test that complex schemas survive the round-trip."""

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="arrow_flight")
        def produce(self):
            return pa.table(
                {
                    "int_col": pa.array([1, None, 3], type=pa.int64()),
                    "str_col": pa.array(["hello", "world", None]),
                    "float_col": pa.array([1.5, 2.5, 3.5], type=pa.float32()),
                }
            )

        def check_schema(self, data):
            schema = data.schema
            return (
                schema.field("int_col").type == pa.int64()
                and schema.field("str_col").type == pa.string()
                and schema.field("float_col").type == pa.float32()
                and data.column("int_col").null_count == 1
                and data.column("str_col").null_count == 1
            )

    actors = [Actor.remote() for _ in range(2)]
    ref = actors[0].produce.remote()
    result = actors[1].check_schema.remote(ref)
    assert ray.get(result)


def test_arrow_flight_transport_cross_node(ray_start_cluster):
    """Test cross-node transfer via Arrow Flight RPC."""
    cluster = ray_start_cluster
    node1 = cluster.add_node(num_cpus=2)
    node2 = cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    node1_id = node1.node_id
    node2_id = node2.node_id

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="arrow_flight")
        def produce(self):
            return pa.table({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})

        def consume(self, data):
            return data.column("a").to_pylist()

        def get_node_id(self):
            return ray.get_runtime_context().get_node_id()

    producer = Actor.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=node1_id, soft=False
        ),
    ).remote()
    consumer = Actor.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=node2_id, soft=False
        ),
    ).remote()

    # Verify actors are on different nodes
    producer_node = ray.get(producer.get_node_id.remote())
    consumer_node = ray.get(consumer.get_node_id.remote())
    assert producer_node != consumer_node

    ref = producer.produce.remote()
    result = consumer.consume.remote(ref)
    assert ray.get(result) == [1, 2, 3]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
