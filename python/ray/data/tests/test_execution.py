import pytest

from typing import List, Any

import ray
from ray.data.block import BlockAccessor
from ray.data._internal.execution.interfaces import ExecutionOptions, RefBundle
from ray.data._internal.execution.bulk_executor import BulkExecutor
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
    inputs = make_ref_bundles(
        [
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9],
        ]
    )
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(lambda block: [b * 2 for b in block], o1)
    o3 = MapOperator(lambda block: [b * -1 for b in block], o2)
    it = executor.execute(o3)
    output = ref_bundles_to_list(it)
    expected = [
        [-2, -4, -6],
        [-8, -10, -12],
        [-14, -16, -18],
    ]
    assert output == expected, (output, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
