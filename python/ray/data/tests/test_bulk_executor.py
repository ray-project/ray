import pytest

from typing import List, Any

import ray
from ray.data._internal.execution.interfaces import ExecutionOptions, RefBundle
from ray.data._internal.execution.bulk_executor import BulkExecutor
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import make_ref_bundles


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


def test_multi_stage_execution():
    executor = BulkExecutor(ExecutionOptions())
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


def test_basic_stats():
    executor = BulkExecutor(ExecutionOptions())
    prev_stats = ray.data.range(10)._plan.stats()
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(
        make_transform(lambda block: [b * 2 for b in block]), o1, name="Foo"
    )
    o3 = MapOperator(
        make_transform(lambda block: [b * 2 for b in block]), o2, name="Bar"
    )
    it = executor.execute(o3, initial_stats=prev_stats)
    output = ref_bundles_to_list(it)
    expected = [[x * 4] for x in range(20)]
    assert output == expected, (output, expected)
    stats_str = executor.get_stats().to_summary().to_string()
    assert "Stage 0 read:" in stats_str, stats_str
    assert "Stage 1 Foo:" in stats_str, stats_str
    assert "Stage 2 Bar:" in stats_str, stats_str
    assert "Extra metrics:" in stats_str, stats_str


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
