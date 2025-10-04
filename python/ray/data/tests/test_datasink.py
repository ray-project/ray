from dataclasses import dataclass
from typing import Iterable, List

import numpy
import pytest

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block
from ray.data.datasource import Datasink
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

    def __init__(self, node_id: str):

        self.num_ok = 0
        self.num_failed = 0
        self.node_id = node_id
        self.num_rows_written = 0

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:

        node_id = ray.get_runtime_context().get_node_id()
        assert node_id == self.node_id

    def on_write_complete(self, write_result: WriteResult[None]):
        self.num_ok += 1
        self.num_rows_written += write_result.num_rows

    def on_write_failed(self, error: Exception) -> None:
        self.num_failed += 1


def test_write_datasink_ray_remote_args(ray_start_cluster):
    ray.shutdown()
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
    )
    bar_worker = cluster.add_node(resources={"bar": 100}, num_cpus=1)
    bar_node_id = bar_worker.node_id

    ray.init(cluster.address)

    output = NodeLoggerOutputDatasink(bar_node_id)
    ds = ray.data.range(100, override_num_blocks=10)
    # Pin write tasks to node with "bar" resource.
    ds.write_datasink(output, ray_remote_args={"resources": {"bar": 1}})
    assert output.num_ok == 1
    assert output.num_failed == 0
    assert output.num_rows_written == 100


@pytest.mark.parametrize("min_rows_per_write", [25, 50])
def test_min_rows_per_write(tmp_path, ray_start_regular_shared, min_rows_per_write):
    class MockDatasink(Datasink[None]):
        def __init__(self, min_rows_per_write):
            self._min_rows_per_write = min_rows_per_write

        def write(self, blocks: Iterable[Block], ctx: TaskContext) -> None:
            assert sum(len(block) for block in blocks) == self._min_rows_per_write

        @property
        def min_rows_per_write(self):
            return self._min_rows_per_write

    ray.data.range(100, override_num_blocks=4).write_datasink(
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
