import pytest
import asyncio
import time
from unittest.mock import MagicMock

from typing import List, Any

import ray
from ray.data.context import DatasetContext
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    RefBundle,
    PhysicalOperator,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.execution.streaming_executor_state import (
    OpState,
    build_streaming_topology,
    process_completed_tasks,
    select_operator_to_run,
)
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import make_ref_bundles


@ray.remote
def sleep():
    time.sleep(999)


def make_transform(block_fn):
    def map_fn(block_iter):
        for block in block_iter:
            yield block_fn(block)

    return map_fn


def ref_bundles_to_list(bundles: List[RefBundle]) -> List[List[Any]]:
    output = []
    for bundle in bundles:
        for block, _ in bundle.blocks:
            output.append(ray.get(block))
    return output


def test_build_streaming_topology():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator(make_transform(lambda block: [b * 2 for b in block]), o2)
    topo, _ = build_streaming_topology(o3, ExecutionOptions())
    assert len(topo) == 3, topo
    assert o1 in topo, topo
    assert not topo[o1].inqueues, topo
    assert topo[o1].outqueue == topo[o2].inqueues[0], topo
    assert topo[o2].outqueue == topo[o3].inqueues[0], topo
    assert list(topo) == [o1, o2, o3]


def test_disallow_non_unique_operators():
    inputs = make_ref_bundles([[x] for x in range(20)])
    # An operator [o1] cannot used in the same DAG twice.
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator(make_transform(lambda block: [b * -1 for b in block]), o1)
    o4 = PhysicalOperator("test_combine", [o2, o3])
    with pytest.raises(ValueError):
        build_streaming_topology(o4, ExecutionOptions())


def test_process_completed_tasks():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(make_transform(lambda block: [b * -1 for b in block]), o1)
    topo, _ = build_streaming_topology(o2, ExecutionOptions())

    # Test processing output bundles.
    assert len(topo[o1].outqueue) == 0, topo
    process_completed_tasks(topo)
    assert len(topo[o1].outqueue) == 20, topo

    # Test processing completed work items.
    sleep_ref = sleep.remote()
    done_ref = ray.put("done")
    o2.get_work_refs = MagicMock(return_value=[sleep_ref, done_ref])
    o2.notify_work_completed = MagicMock()
    o2.inputs_done = MagicMock()
    process_completed_tasks(topo)
    o2.notify_work_completed.assert_called_once_with(done_ref)
    o2.inputs_done.assert_not_called()

    # Test input finalization.
    o2.get_work_refs = MagicMock(return_value=[done_ref])
    o2.notify_work_completed = MagicMock()
    o2.inputs_done = MagicMock()
    o1.completed = MagicMock(return_value=True)
    topo[o1].outqueue.clear()
    process_completed_tasks(topo)
    o2.notify_work_completed.assert_called_once_with(done_ref)
    o2.inputs_done.assert_called_once()


def test_select_operator_to_run():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator(make_transform(lambda block: [b * 2 for b in block]), o2)
    topo, _ = build_streaming_topology(o3, ExecutionOptions())

    # Test empty.
    assert select_operator_to_run(topo) is None

    # Test backpressure based on queue length between operators.
    topo[o1].outqueue.append("dummy1")
    assert select_operator_to_run(topo) == o2
    topo[o1].outqueue.append("dummy2")
    assert select_operator_to_run(topo) == o2
    topo[o2].outqueue.append("dummy3")
    assert select_operator_to_run(topo) == o3

    # Test backpressure includes num active tasks as well.
    topo[o3].num_active_tasks = MagicMock(return_value=2)
    assert select_operator_to_run(topo) == o2
    topo[o2].num_active_tasks = MagicMock(return_value=2)
    assert select_operator_to_run(topo) == o3


def test_dispatch_next_task():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o1_state = OpState(o1, [])
    o2 = MapOperator(make_transform(lambda block: [b * -1 for b in block]), o1)
    op_state = OpState(o2, [o1_state.outqueue])

    # TODO: test multiple inqueues with the union operator.
    op_state.inqueues[0].append("dummy1")
    op_state.inqueues[0].append("dummy2")

    o2.add_input = MagicMock()
    op_state.dispatch_next_task()
    assert o2.add_input.called_once_with("dummy1")

    o2.add_input = MagicMock()
    op_state.dispatch_next_task()
    assert o2.add_input.called_once_with("dummy2")


def test_pipelined_execution():
    executor = StreamingExecutor(ExecutionOptions())
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator(make_transform(lambda block: [b * 2 for b in block]), o2)

    def reverse_sort(inputs: List[RefBundle]):
        reversed_list = inputs[::-1]
        return reversed_list, {}

    o4 = AllToAllOperator(reverse_sort, o3)
    it = executor.execute(o4)
    output = ref_bundles_to_list(it)
    expected = [[x * -2] for x in range(20)][::-1]
    assert output == expected, (output, expected)


# TODO(ekl) remove this test once we have the new backend on by default.
def test_e2e_streaming_sanity():
    DatasetContext.get_current().new_execution_backend = True
    DatasetContext.get_current().use_streaming_executor = True

    @ray.remote
    class Barrier:
        async def admit(self, x):
            if x == 4:
                print("Not allowing 4 to pass")
                await asyncio.sleep(999)
            else:
                print(f"Allowing {x} to pass")

    barrier = Barrier.remote()

    def f(x):
        ray.get(barrier.admit.remote(x))
        return x + 1

    # Check we can take the first items even if the last one gets stuck.
    result = ray.data.range(5, parallelism=5).map(f)
    assert result.take(4) == [1, 2, 3, 4]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
