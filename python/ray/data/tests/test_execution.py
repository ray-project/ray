import pytest

import time
from typing import List, Any

import ray
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.interfaces import ExecutionOptions, RefBundle
from ray.data._internal.execution.bulk_executor import BulkExecutor
from ray.data._internal.execution.pipelined_executor import PipelinedExecutor
from ray.data._internal.execution.operators import (
    InputDataBuffer,
    MapOperator,
    _from_dataset_read_tasks,
)
from ray.data._internal.execution.util import _make_ref_bundles


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
    o2 = MapOperator(lambda block: [b * -1 for b in block], o1)
    o3 = MapOperator(lambda block: [b * 2 for b in block], o2)
    it = executor.execute(o3)
    output = sorted(ref_bundles_to_list(it))  # TODO: preserve order option
    expected = sorted([[x * -2] for x in range(20)])
    assert output == expected, (output, expected)


def test_actor_strategy():
    executor = PipelinedExecutor(ExecutionOptions())
    inputs = _make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(lambda block: [b * -1 for b in block], o1)
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


def test_ds_adapter():
    executor = PipelinedExecutor(
        ExecutionOptions(parallelism_limit=3, locality_with_output=True)
    )
    o1 = _from_dataset_read_tasks(ray.data.range(20))
    o2 = MapOperator(lambda block: [b * -1 for b in block], o1, name="Negate")
    o3 = MapOperator(s(0.3, lambda block: [b * 2 for b in block]), o2, name="Multiply")
    it = executor.execute(o3)
    output = ref_bundles_to_list(it)
    expected = [[x * -2] for x in range(20)]
    assert sorted(output) == sorted(expected), (output, expected)


def s(s, f):
    def func(x):
        time.sleep(s)
        return f(x)

    return func


def test_basic_pipelined():
    executor = PipelinedExecutor(ExecutionOptions())
    inputs = _make_ref_bundles([[x] for x in range(100)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(s(0.05, lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator(s(0.3, lambda block: [b * 2 for b in block]), o2)
    o4 = MapOperator(s(0.05, lambda block: [b * 1 for b in block]), o3)
    o5 = MapOperator(s(0.2, lambda block: [b * 1 for b in block]), o4)
    it = executor.execute(o5)
    output = ref_bundles_to_list(it)
    expected = [[x * -2] for x in range(100)]
    assert sorted(output) == sorted(expected), (output, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
