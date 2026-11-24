import time

import pyarrow as pa
import pytest

import ray
from ray._private.internal_api import (
    get_local_ongoing_lineage_reconstruction_tasks,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.tests.conftest import *  # noqa
from ray.tests.conftest import wait_for_condition


@pytest.mark.parametrize("accounting_enabled", [True, False])
def test_reconstruction_usage_reaches_resource_manager_during_execution(
    ray_start_cluster_enabled, restore_data_context, accounting_enabled
):
    """Reconstruction tasks show up in the resource manager while the pipeline runs.

    The head has no CPUs and the worker has one, so the operator has at most one
    task in flight and the worker runs them one at a time. Once the first
    ``num_finished`` tasks complete and the next is running, removing the worker
    leaves exactly one own task retrying and ``num_finished`` outputs
    reconstructing, all pending because nothing can host ``worker``.
    """
    data_context = ray.data.DataContext.get_current()
    data_context.enable_lineage_reconstruction_resource_accounting = accounting_enabled
    num_finished = 2

    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=0, resources={"head": 10})
    ray.init(address=cluster.address)
    worker = cluster.add_node(num_cpus=1, resources={"worker": 10})

    @ray.remote(num_cpus=0, resources={"head": 1})
    class Gate:
        """Lets the first ``let_through`` map calls run and parks the rest."""

        def __init__(self, let_through: int):
            self._let_through = let_through
            self._entered = 0
            self._open = False

        def enter(self) -> int:
            self._entered += 1
            return self._entered

        def should_wait(self, ticket: int) -> bool:
            return ticket > self._let_through and not self._open

        def num_entered(self) -> int:
            return self._entered

        def open(self) -> None:
            self._open = True

    gate = Gate.remote(num_finished)  # pyrefly: ignore[missing-attribute]

    def fn(batch):
        ticket = ray.get(gate.enter.remote())
        while ray.get(gate.should_wait.remote(ticket)):
            time.sleep(0.1)
        return batch

    # 1 MiB puts the outputs in the worker's plasma, so they're lost with the node.
    blocks = [
        pa.Table.from_pylist([{"data": "x" * (1 << 20)}])
        for _ in range(num_finished + 1)
    ]
    # User labels go through `.options()`, which replaces `_labels` wholesale;
    # the operator id must survive that or reconstruction tasks go uncounted.
    ds = ray.data.from_blocks(blocks).map_batches(
        fn,
        batch_size=None,
        num_cpus=1,
        resources={"worker": 1},
        _labels={"team": "ingest"},
    )

    # Don't consume: finished outputs must stay referenced, and the scheduling
    # loop must stay alive so usage keeps refreshing.
    bundles, _, executor = ds._execute_to_iterator()
    assert executor is not None and executor._topology is not None
    map_op = next(
        op for op in executor._topology if isinstance(op, TaskPoolMapOperator)
    )
    resource_manager = executor._resource_manager

    # Task ``num_finished + 1`` entering ``fn`` means every earlier task finished.
    wait_for_condition(
        lambda: ray.get(gate.num_entered.remote()) == num_finished + 1, timeout=30
    )

    cluster.remove_node(worker)

    # Wait until Core is actually reconstructing, so the disabled case is a real control.
    wait_for_condition(
        lambda: sum(n for _, n in get_local_ongoing_lineage_reconstruction_tasks())
        == num_finished,
        timeout=30,
    )
    own = 1  # the interrupted task, now retrying
    expected_cpu = own + (num_finished if accounting_enabled else 0)
    wait_for_condition(
        lambda: resource_manager.get_op_usage(map_op).cpu == expected_cpu, timeout=30
    )
    # A single match may predate the loop seeing the reconstruction tasks; hold
    # it across several iterations so a stale read can't pass the control.
    for _ in range(5):
        assert resource_manager.get_op_usage(map_op).cpu == expected_cpu
        time.sleep(0.2)

    ray.get(gate.open.remote())
    cluster.add_node(num_cpus=1, resources={"worker": 10})
    # Draining proves only the retried task finished (earlier bundles already
    # carry metadata); Core reporting nothing in flight proves the rest.
    wait_for_condition(
        lambda: not get_local_ongoing_lineage_reconstruction_tasks(), timeout=30
    )
    assert sum(bundle.num_rows() for bundle in bundles) == num_finished + 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
