import gc
from typing import List

import pandas as pd
import pytest

import ray
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    RefBundle,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.progress.base_progress import NoopSubProgressBar
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.tests.util import (
    _get_blocks,
    _mul2_transform,
    _take_outputs,
    create_map_transformer_from_block_fn,
    run_one_op_task,
    run_op_tasks_sync,
)
from ray.tests.conftest import *  # noqa

_mul2_map_data_prcessor = create_map_transformer_from_block_fn(_mul2_transform)


def test_name_and_repr(ray_start_regular_shared):
    inputs = make_ref_bundles([[1, 2], [3], [4, 5]])
    input_op = InputDataBuffer(DataContext.get_current(), inputs)
    map_op1 = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op,
        DataContext.get_current(),
        name="map1",
    )

    assert map_op1.name == "map1"
    assert map_op1.dag_str == "InputDataBuffer[Input] -> TaskPoolMapOperator[map1]"
    assert str(map_op1) == "TaskPoolMapOperator[map1]"

    map_op2 = MapOperator.create(
        _mul2_map_data_prcessor,
        map_op1,
        DataContext.get_current(),
        name="map2",
    )
    assert map_op2.name == "map2"
    assert (
        map_op2.dag_str
        == "InputDataBuffer[Input] -> TaskPoolMapOperator[map1] -> TaskPoolMapOperator[map2]"
    )
    assert str(map_op2) == "TaskPoolMapOperator[map2]"


def test_input_data_buffer(ray_start_regular_shared):
    # Create with bundles.
    inputs = make_ref_bundles([[1, 2], [3], [4, 5]])
    op = InputDataBuffer(DataContext.get_current(), inputs)

    # Check we return all bundles in order.
    assert not op.has_completed()
    assert _take_outputs(op) == [[1, 2], [3], [4, 5]]
    assert op.has_completed()


def test_all_to_all_operator():
    def dummy_all_transform(bundles: List[RefBundle], ctx):
        assert len(ctx.sub_progress_bar_dict) == 2
        assert list(ctx.sub_progress_bar_dict.keys()) == ["Test1", "Test2"]
        return make_ref_bundles([[1, 2], [3, 4]]), {"FooStats": []}

    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(100)])
    )
    op = AllToAllOperator(
        dummy_all_transform,
        input_op,
        DataContext.get_current(),
        target_max_block_size_override=DataContext.get_current().target_max_block_size,
        num_outputs=2,
        sub_progress_bar_names=["Test1", "Test2"],
        name="TestAll",
    )

    # Initialize progress bar.
    for name in op.get_sub_progress_bar_names():
        pg = NoopSubProgressBar(
            name=name,
            max_name_length=100,
        )
        op.set_sub_progress_bar(name, pg)

    # Feed data.
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()

    # Check we return transformed bundles.
    assert not op.has_completed()
    outputs = _take_outputs(op)
    expected = [[1, 2], [3, 4]]
    assert sorted(outputs) == expected, f"Expected {expected}, got {outputs}"
    stats = op.get_stats()
    assert "FooStats" in stats
    assert op.has_completed()


def test_num_outputs_total():
    # The number of outputs is always known for InputDataBuffer.
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(100)])
    )
    assert input_op.num_outputs_total() == 100

    # Prior to execution, the number of outputs is unknown
    # for Map/AllToAllOperator operators.
    op1 = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op,
        DataContext.get_current(),
        name="TestMapper",
    )
    assert op1.num_outputs_total() is None

    def dummy_all_transform(bundles: List[RefBundle]):
        return make_ref_bundles([[1, 2], [3, 4]]), {"FooStats": []}

    op2 = AllToAllOperator(
        dummy_all_transform,
        input_op=op1,
        data_context=DataContext.get_current(),
        target_max_block_size_override=DataContext.get_current().target_max_block_size,
        name="TestAll",
    )
    assert op2.num_outputs_total() is None

    # Feed data and implement streaming exec.
    output = []
    op1.start(ExecutionOptions(actor_locality_enabled=True))
    while input_op.has_next():
        op1.add_input(input_op.get_next(), 0)
        while not op1.has_next():
            run_one_op_task(op1)
        while op1.has_next():
            ref = op1.get_next()
            assert ref.owns_blocks, ref
            _get_blocks(ref, output)
    # After op finishes, num_outputs_total is known.
    assert op1.num_outputs_total() == 100


def test_all_to_all_estimated_num_output_bundles():
    # Test all to all operator
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(100)])
    )

    def all_transform(bundles: List[RefBundle], ctx):
        return bundles, {}

    estimated_output_blocks = 500
    op1 = AllToAllOperator(
        all_transform,
        input_op,
        DataContext.get_current(),
        DataContext.get_current().target_max_block_size,
        estimated_output_blocks,
    )
    op2 = AllToAllOperator(
        all_transform,
        op1,
        DataContext.get_current(),
        DataContext.get_current().target_max_block_size,
    )

    while input_op.has_next():
        op1.add_input(input_op.get_next(), 0)
    op1.all_inputs_done()
    run_op_tasks_sync(op1)

    while op1.has_next():
        op2.add_input(op1.get_next(), 0)
    op2.all_inputs_done()
    run_op_tasks_sync(op2)

    # estimated output blocks for op2 should fallback to op1
    assert op2._estimated_num_output_bundles is None
    assert op2.num_outputs_total() == estimated_output_blocks


def test_input_data_buffer_does_not_free_inputs():
    # Tests https://github.com/ray-project/ray/issues/46282
    block = pd.DataFrame({"id": [0]})
    block_ref = ray.put(block)
    metadata = BlockAccessor.for_block(block).get_metadata()
    schema = BlockAccessor.for_block(block).schema()
    op = InputDataBuffer(
        DataContext.get_current(),
        input_data=[
            RefBundle([(block_ref, metadata)], owns_blocks=False, schema=schema)
        ],
    )

    op.get_next()
    gc.collect()

    # `InputDataBuffer` should still hold a reference to the input block even after
    # `get_next` is called.
    assert len(gc.get_referrers(block_ref)) > 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
