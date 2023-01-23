import pytest

import time
from typing import List, Any

import ray
from ray.data.context import DatasetContext
from ray.data._internal.compute import ActorPoolStrategy
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


@pytest.mark.parametrize("preserve_order", [False, True])
def test_multi_stage_execution(preserve_order):
    executor = BulkExecutor(ExecutionOptions(preserve_order=preserve_order))
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)

    def delay_first(block):
        if block[0] == 0:
            print("Delaying first block to force de-ordering")
            time.sleep(2)
        result = [b * -1 for b in block]
        return result

    o2 = MapOperator(make_transform(delay_first), o1)
    o3 = MapOperator(make_transform(lambda block: [b * 2 for b in block]), o2)

    def reverse_sort(inputs: List[RefBundle]):
        reversed_list = inputs[::-1]
        return reversed_list, {}

    o4 = AllToAllOperator(reverse_sort, o3)
    it = executor.execute(o4)
    output = ref_bundles_to_list(it)
    expected = [[x * -2] for x in range(20)][::-1]
    if preserve_order:
        assert output == expected, (output, expected)
    else:
        assert output != expected, (output, expected)
        assert sorted(output) == sorted(expected), (output, expected)


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


# TODO(ekl) remove this test once we have the new backend on by default.
def test_e2e_bulk_sanity():
    DatasetContext.get_current().new_execution_backend = True
    result = ray.data.range(5).map(lambda x: x + 1)
    assert result.take_all() == [1, 2, 3, 4, 5], result

    # Checks new executor was enabled.
    assert "obj_store_mem_alloc" in result.stats(), result.stats()


def test_actor_strategy():
    executor = BulkExecutor(ExecutionOptions())
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator(
        make_transform(lambda block: [b * 2 for b in block]),
        o2,
        compute_strategy=ActorPoolStrategy(1, 2),
        ray_remote_args={"num_cpus": 1},
        name="ActorMap",
    )
    it = executor.execute(o3)
    output = ref_bundles_to_list(it)
    expected = [[x * -2] for x in range(20)]
    assert sorted(output) == sorted(expected), (output, expected)


def test_new_execution_backend_invocation():
    DatasetContext.get_current().new_execution_backend = True
    # Read-only: will use legacy executor for now.
    ds = ray.data.range(10)
    assert ds.take_all() == list(range(10))
    # read->randomize_block_order: will use new executor, although it's also
    # a read-equivalent once fused.
    ds = ray.data.range(10).randomize_block_order()
    assert set(ds.take_all()) == set(range(10))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
