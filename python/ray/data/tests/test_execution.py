import pytest

import time
from typing import List, Any

import ray
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.interfaces import ExecutionOptions, RefBundle
from ray.data._internal.execution.bulk_executor import BulkExecutor
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import _make_ref_bundles


def s(s, f):
    def func(x):
        time.sleep(s)
        return f(x)

    return make_transform(func)


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


def test_basic_bulk():
    executor = BulkExecutor(ExecutionOptions())
    inputs = _make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator(make_transform(lambda block: [b * 2 for b in block]), o2)
    it = executor.execute(o3)
    output = ref_bundles_to_list(it)
    expected = [[x * -2] for x in range(20)]
    assert output == expected, (output, expected)


def test_block_bundling():
    executor = BulkExecutor(ExecutionOptions())
    inputs = _make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(
        make_transform(lambda block: [b * -1 for b in block]), o1, target_block_size=3
    )
    o3 = MapOperator(
        make_transform(lambda block: [b * 2 for b in block]), o2, target_block_size=3
    )
    it = executor.execute(o3)
    # For 20 blocks, 1 row per block and target_block_size=3, there will be 7 tasks
    # launched.
    assert o3._execution_state._next_task_index == 7
    output = ref_bundles_to_list(it)
    expected = [[x * -2] for x in range(20)]
    assert output == expected, (output, expected)


def test_actor_strategy():
    executor = BulkExecutor(ExecutionOptions())
    inputs = _make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator(
        s(0.8, lambda block: [b * 2 for b in block]),
        o2,
        compute_strategy=ActorPoolStrategy(1, 2),
        ray_remote_args={"num_cpus": 1},
        name="ActorMap",
    )
    it = executor.execute(o3)
    output = ref_bundles_to_list(it)
    expected = [[x * -2] for x in range(20)]
    assert sorted(output) == sorted(expected), (output, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
