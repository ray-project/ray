from dataclasses import dataclass
from typing import Iterable, List

import numpy
import pytest

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource import Datasink, MultiDatasink
from ray.data.datasource.datasink import DummyOutputDatasink, WriteResult


def test_write_datasink(ray_start_regular_shared):
    output = DummyOutputDatasink()
    ds = ray.data.range(10, override_num_blocks=2)
    ds.write_datasink(output)
    assert output.num_ok == 1
    assert output.num_failed == 0
    assert ray.get(output.data_sink.get_rows_written.remote()) == 10

    output.enabled = False
    ds = ray.data.range(10, override_num_blocks=2)
    with pytest.raises(ValueError):
        ds.write_datasink(output, ray_remote_args={"max_retries": 0})
    assert output.num_ok == 1
    assert output.num_failed == 1
    assert ray.get(output.data_sink.get_rows_written.remote()) == 10


class NodeLoggerOutputDatasink(Datasink[None]):
    """A writable datasource that logs node IDs of write tasks, for testing."""

    def __init__(self):
        @ray.remote
        class DataSink:
            def __init__(self):
                self.rows_written = 0
                self.node_ids = set()

            def write(self, node_id: str, block: Block) -> str:
                block = BlockAccessor.for_block(block)
                self.rows_written += block.num_rows()
                self.node_ids.add(node_id)

            def get_rows_written(self):
                return self.rows_written

            def get_node_ids(self):
                return self.node_ids

        self.data_sink = DataSink.remote()
        self.num_ok = 0
        self.num_failed = 0

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        data_sink = self.data_sink

        def write(b):
            node_id = ray.get_runtime_context().get_node_id()
            return data_sink.write.remote(node_id, b)

        tasks = []
        for b in blocks:
            tasks.append(write(b))
        ray.get(tasks)

    def on_write_complete(self, write_result: WriteResult[None]):
        self.num_ok += 1

    def on_write_failed(self, error: Exception) -> None:
        self.num_failed += 1


def test_write_datasink_ray_remote_args(ray_start_cluster):
    ray.shutdown()
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
    )
    cluster.add_node(resources={"bar": 100}, num_cpus=1)

    ray.init(cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    bar_node_id = ray.get(get_node_id.options(resources={"bar": 1}).remote())

    output = NodeLoggerOutputDatasink()
    ds = ray.data.range(100, override_num_blocks=10)
    # Pin write tasks to node with "bar" resource.
    ds.write_datasink(output, ray_remote_args={"resources": {"bar": 1}})
    assert output.num_ok == 1
    assert output.num_failed == 0
    assert ray.get(output.data_sink.get_rows_written.remote()) == 100

    node_ids = ray.get(output.data_sink.get_node_ids.remote())
    assert node_ids == {bar_node_id}


@pytest.mark.parametrize("min_rows_per_write", [5, 10, 50])
def test_min_rows_per_write(tmp_path, ray_start_regular_shared, min_rows_per_write):
    class MockDatasink(Datasink[None]):
        def __init__(self, min_rows_per_write):
            self._min_rows_per_write = min_rows_per_write

        def write(self, blocks: Iterable[Block], ctx: TaskContext) -> None:
            assert sum(len(block) for block in blocks) == self._min_rows_per_write

        @property
        def min_rows_per_write(self):
            return self._min_rows_per_write

    ray.data.range(100, override_num_blocks=20).write_datasink(
        MockDatasink(min_rows_per_write)
    )


def test_write_result(ray_start_regular_shared):
    """Test the write_result argument in `on_write_complete`."""

    @dataclass
    class CustomWriteResult:

        ids: List[int]

    class CustomDatasink(Datasink[CustomWriteResult]):
        def __init__(self) -> None:
            self.ids = []
            self.num_rows = 0
            self.size_bytes = 0

        def write(self, blocks: Iterable[Block], ctx: TaskContext):
            ids = []
            for b in blocks:
                ids.extend(b["id"].to_pylist())
            return CustomWriteResult(ids=ids)

        def on_write_complete(self, write_result: WriteResult[CustomWriteResult]):
            ids = []
            for result in write_result.write_returns:
                ids.extend(result.ids)
            self.ids = sorted(ids)
            self.num_rows = write_result.num_rows
            self.size_bytes = write_result.size_bytes

    num_items = 100
    size_bytes_per_row = 1000

    def map_fn(row):
        row["data"] = numpy.zeros(size_bytes_per_row, dtype=numpy.int8)
        return row

    ds = ray.data.range(num_items).map(map_fn)

    datasink = CustomDatasink()
    ds.write_datasink(datasink)

    assert datasink.ids == list(range(num_items))
    assert datasink.num_rows == num_items
    assert datasink.size_bytes == pytest.approx(num_items * size_bytes_per_row, rel=0.1)


def test_multi_datasink(ray_start_regular_shared):
    """Test the basic functionality of MultiDatasink."""
    sink1 = DummyOutputDatasink()
    sink2 = DummyOutputDatasink()
    multi_sink = MultiDatasink({"sink1": sink1, "sink2": sink2})

    ds = ray.data.range(10, override_num_blocks=2)
    ds.write_datasink(multi_sink)

    # Check that both sinks have written data successfully
    assert sink1.num_ok == 1
    assert sink1.num_failed == 0
    assert ray.get(sink1.data_sink.get_rows_written.remote()) == 10

    assert sink2.num_ok == 1
    assert sink2.num_failed == 0
    assert ray.get(sink2.data_sink.get_rows_written.remote()) == 10


def test_multi_datasink_partial_failure(ray_start_regular_shared):
    """Test the partial failure scenario of MultiDatasink."""
    sink1 = DummyOutputDatasink()
    sink2 = DummyOutputDatasink()
    # Disable the second sink
    sink2.enabled = False
    multi_sink = MultiDatasink({"sink1": sink1, "sink2": sink2})

    ds = ray.data.range(10, override_num_blocks=2)
    # The first sink should write successfully, while the second sink will fail
    with pytest.raises(ValueError):
        ds.write_datasink(multi_sink, ray_remote_args={"max_retries": 0})

    # Check that the first sink has written data successfully
    assert sink1.num_ok == 1
    assert sink1.num_failed == 0
    assert ray.get(sink1.data_sink.get_rows_written.remote()) == 10

    # The second sink should fail without affecting the overall operation
    assert sink2.num_ok == 0
    assert sink2.num_failed == 1


def test_multi_datasink_different_types(ray_start_regular_shared):
    """Test that MultiDatasink supports sinks with different return types."""

    @dataclass
    class CustomWriteResult:
        ids: List[int]

    class CustomDatasink(Datasink[CustomWriteResult]):
        def __init__(self) -> None:
            self.ids = []
            self.num_ok = 0
            self.num_failed = 0

        def write(self, blocks: Iterable[Block], ctx: TaskContext):
            ids = []
            for b in blocks:
                ids.extend(b["id"].to_pylist())
            return CustomWriteResult(ids=ids)

        def on_write_complete(self, write_result: WriteResult[CustomWriteResult]):
            self.num_ok += 1
            ids = []
            for result in write_result.write_returns:
                ids.extend(result.ids)
            self.ids = sorted(ids)

        def on_write_failed(self, error: Exception) -> None:
            self.num_failed += 1

    # Create two sinks of different types
    standard_sink = DummyOutputDatasink()
    custom_sink = CustomDatasink()

    # Create MultiDatasink
    multi_sink = MultiDatasink({"standard": standard_sink, "custom": custom_sink})

    # Write data
    ds = ray.data.range(10, override_num_blocks=2)
    ds.write_datasink(multi_sink)

    # Verify that both sinks have processed the data correctly
    assert standard_sink.num_ok == 1
    assert standard_sink.num_failed == 0
    assert ray.get(standard_sink.data_sink.get_rows_written.remote()) == 10

    assert custom_sink.num_ok == 1
    assert custom_sink.num_failed == 0
    assert custom_sink.ids == list(range(10))


def test_multi_datasink_min_rows_property(ray_start_regular_shared):
    """Test that MultiDatasink correctly handles the min_rows_per_write property."""

    class MockDatasink(Datasink[None]):
        def __init__(self, min_rows_per_write):
            self._min_rows_per_write = min_rows_per_write

        def write(self, blocks: Iterable[Block], ctx: TaskContext) -> None:
            pass

        @property
        def min_rows_per_write(self):
            return self._min_rows_per_write

    # Create sinks with different min_rows_per_write values
    sink1 = MockDatasink(5)
    sink2 = MockDatasink(10)
    sink3 = MockDatasink(None)

    # Test with only two sinks that have values
    multi_sink1 = MultiDatasink({"sink1": sink1, "sink2": sink2})
    assert multi_sink1.min_rows_per_write == 10

    # Test with a sink that has None value
    multi_sink2 = MultiDatasink({"sink1": sink1, "sink3": sink3})
    assert multi_sink2.min_rows_per_write == 5

    # Test with all sinks having None values
    multi_sink3 = MultiDatasink({"sink3": sink3})
    assert multi_sink3.min_rows_per_write is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
