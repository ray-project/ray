import pytest

from typing import List, Any

import ray
from ray.data.block import BlockAccessor
from ray.data._internal.execution.interfaces import ExecutionOptions, RefBundle
from ray.data._internal.execution.bulk_executor import BulkExecutor
from ray.data._internal.execution.pipelined_executor import PipelinedExecutor
from ray.data._internal.execution.operators import InputDataBuffer, MapOperator


def make_ref_bundles(simple_data: List[List[Any]]) -> List[RefBundle]:
    output = []
    for block in simple_data:
        output.append(
            RefBundle(
                [
                    (
                        ray.put(block),
                        BlockAccessor.for_block(block).get_metadata([], None),
                    )
                ]
            )
        )
    return output


def ref_bundles_to_list(bundles: List[RefBundle]) -> List[List[Any]]:
    output = []
    for bundle in bundles:
        for block, _ in bundle.blocks:
            output.append(ray.get(block))
    return output


def test_basic_bulk():
    executor = BulkExecutor(ExecutionOptions())
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(lambda block: [b * -1 for b in block], o1)
    o3 = MapOperator(lambda block: [b * 2 for b in block], o2)
    it = executor.execute(o3)
    output = ref_bundles_to_list(it)
    expected = [[x * -2] for x in range(20)]
    assert output == expected, (output, expected)


def test_basic_pipelined():
    executor = PipelinedExecutor(ExecutionOptions())
    inputs = make_ref_bundles([[x] for x in range(100)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(lambda block: [b * -1 for b in block], o1)
    o3 = MapOperator(lambda block: [b * 2 for b in block], o2)
    o4 = MapOperator(lambda block: [b * 1 for b in block], o3)
    o5 = MapOperator(lambda block: [b * 1 for b in block], o4)
    it = executor.execute(o5)
    output = ref_bundles_to_list(it)
    expected = [[x * -2] for x in range(100)]
    assert sorted(output) == sorted(expected), (output, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
