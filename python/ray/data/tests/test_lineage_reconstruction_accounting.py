import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.tests.conftest import *  # noqa
from ray.tests.conftest import wait_for_condition


def test_task_pool_map_operator_counts_lineage_reconstruction_tasks(
    ray_start_cluster_enabled,
):
    """Lineage reconstruction tasks count toward operator usage."""
    data_context = ray.data.DataContext.get_current()
    zero = ExecutionResources.zero()

    # A head node and a single worker node.
    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=4, resources={"head": 10})
    ray.init(address=cluster.address)
    worker = cluster.add_node(num_cpus=4, resources={"worker": 10})

    # Big enough to land in plasma, which is what makes the map output
    # reconstructable. `from_blocks` keeps the input off the worker node.
    block = pa.Table.from_pylist([{"data": "x" * 4 * 1024 * 1024}])
    ds = ray.data.from_blocks([block]).map_batches(
        lambda batch: batch, num_cpus=1, resources={"worker": 1}
    )

    physical_plan, _ = get_execution_plan(ds._logical_plan)
    map_op = physical_plan.dag
    assert isinstance(map_op, TaskPoolMapOperator)

    # Holding the bundles keeps the outputs referenced, which keeps
    # the task that made them pinned in Ray Core's lineage.
    output_bundles = []
    executor = StreamingExecutor(data_context)
    for bundle in executor.execute(map_op):
        output_bundles.append(bundle)
    assert output_bundles

    # Nothing is lost yet, so there's no extra usage to report.
    assert map_op.extra_resource_usage() == zero

    # Removing the node triggers reconstruction. Nothing can host the
    # replay while it's gone, so the task stays pending.
    cluster.remove_node(worker)
    wait_for_condition(lambda: map_op.extra_resource_usage().cpu == 1)

    # Re-add the node. Reconstruction finishes and the usage clears.
    cluster.add_node(num_cpus=4, resources={"worker": 10})
    wait_for_condition(lambda: map_op.extra_resource_usage() == zero)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
