from typing import Iterable

import pytest

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource import Datasink
from ray.data.datasource.datasink import DummyOutputDatasink, WriteResult


def test_write_datasink(ray_start_regular_shared):
    output = DummyOutputDatasink()
    ds = ray.data.range(10, override_num_blocks=2)
    ds.write_datasink(output)
    # assert output.num_ok == 1
    # assert output.num_failed == 0
    assert ray.get(output.data_sink.get_rows_written.remote()) == 10

    output.enabled = False
    ds = ray.data.range(10, override_num_blocks=2)
    with pytest.raises(ValueError):
        ds.write_datasink(output, ray_remote_args={"max_retries": 0})
    # assert output.num_ok == 1
    # assert output.num_failed == 1
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

    def on_write_complete(self, _: WriteResult[None]):
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


@pytest.mark.parametrize("num_rows_per_write", [5, 10, 50])
def test_num_rows_per_write(tmp_path, ray_start_regular_shared, num_rows_per_write):
    class MockDatasink(Datasink[None]):
        def __init__(self, num_rows_per_write):
            self._num_rows_per_write = num_rows_per_write

        def write(self, blocks: Iterable[Block], ctx: TaskContext) -> None:
            assert sum(len(block) for block in blocks) == self._num_rows_per_write

        @property
        def num_rows_per_write(self):
            return self._num_rows_per_write

    ray.data.range(100, override_num_blocks=20).write_datasink(
        MockDatasink(num_rows_per_write)
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
