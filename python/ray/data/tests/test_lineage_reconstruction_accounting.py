import time
from typing import List

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionResources,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
    MapTransformFn,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data.block import BlockAccessor
from ray.tests.conftest import *  # noqa
from ray.tests.conftest import wait_for_condition


def test_task_pool_map_operator_counts_lineage_reconstruction_tasks(
    ray_start_cluster_enabled, disable_timed_cache_fixture
):
    """Tasks Ray Core runs to rebuild a lost output count toward operator usage.

    `disable_timed_cache_fixture` is necessary because this test needs the
    most up-to-date values from `get_local_ongoing_lineage_reconstruction_tasks`.
    """
    data_context = ray.data.DataContext.get_current()

    # Create a cluster with a head node and a single worker node.
    cluster = ray_start_cluster_enabled
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker = cluster.add_node(resources={"worker": 1})

    # Create an input data operator with a single block as input. The block is
    # large enough that losing it is expensive to hide.
    block = pa.Table.from_pylist([{"data": "\x00" * 128 * 1024 * 1024}])
    block_ref = ray.put(block)
    metadata = BlockAccessor.for_block(block).get_metadata()
    schema = BlockAccessor.for_block(block).schema()
    bundle = RefBundle(
        # pyrefly: ignore[bad-argument-type]
        (BlockEntry(ref=block_ref, metadata=metadata),),
        owns_blocks=False,
        schema=schema,
    )
    input_op = InputDataBuffer(data_context, [bundle])

    # Create a signal actor so the map only finishes when we want it to.
    @ray.remote(num_cpus=0, resources={"head": 1})
    class Signal:
        def __init__(self, is_map_blocked: bool):
            self._is_map_blocked = is_map_blocked

        def block_map(self):
            print("Blocking map function")
            self._is_map_blocked = True

        def unblock_map(self):
            print("Unblocking map function")
            self._is_map_blocked = False

        def is_map_blocked(self) -> bool:
            return self._is_map_blocked

    # Start with the transform function unblocked.
    signal = Signal.remote(False)  # pyrefly: ignore[missing-attribute]

    def block_fn(block, _):
        print("Entering block function")

        while ray.get(signal.is_map_blocked.remote()):
            print("Waiting for map to be unblocked")
            time.sleep(0.1)

        print("Exiting block function")
        return block

    transform_fns: List[MapTransformFn] = [BlockMapTransformFn(block_fn)]
    map_transformer = MapTransformer(transform_fns)
    map_op = MapOperator.create(
        map_transformer,
        input_op,
        data_context,
        ray_remote_args={"resources": {"worker": 1}, "num_cpus": 1},
    )
    assert isinstance(map_op, TaskPoolMapOperator)

    output_bundles = []
    executor = StreamingExecutor(data_context)
    for bundle in executor.execute(map_op):
        output_bundles.append(bundle)

    # Nothing is lost yet, so there's no extra usage to report.
    assert map_op.extra_resource_usage() == ExecutionResources.zero()

    # Remove the node to trigger lineage reconstruction. The map function stays
    # blocked so the reconstruction task remains in flight while we observe it.
    ray.get(signal.block_map.remote())
    cluster.remove_node(worker)
    wait_for_condition(lambda: map_op.extra_resource_usage().cpu == 1, timeout=10)

    # Re-add the node and unblock the map function. Once reconstruction
    # finishes, the extra usage goes away again.
    ray.get(signal.unblock_map.remote())
    cluster.add_node(resources={"worker": 1})
    wait_for_condition(
        lambda: map_op.extra_resource_usage() == ExecutionResources.zero(), timeout=10
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
