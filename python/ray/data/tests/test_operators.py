import gc
from typing import List
from unittest.mock import MagicMock

import pandas as pd
import pytest

import ray
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
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
from ray.data.block import BlockAccessor, BlockMetadata
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


class TestAllToAllOperatorBlockRefCounter:
    """Tests for BlockRefCounter integration in AllToAllOperator.all_inputs_done().

    Uses plain object() as fake ObjectRefs — RefBundle validation is bypassed by
    populating _input_buffer directly with _FakeBundle stubs. No Ray required.
    """

    class _FakeBundle:
        """Minimal RefBundle stand-in for all_inputs_done tests.

        Exposes .blocks, .block_refs, num_rows(), and size_bytes() so that
        FIFOBundleQueue metrics tracking doesn't crash on add().
        """

        def __init__(self, blocks):
            self.blocks = tuple(blocks)
            self.block_refs = [ref for ref, _ in self.blocks]

        def num_rows(self):
            return len(self.blocks)

        def size_bytes(self):
            return sum(m.size_bytes for _, m in self.blocks)

    def _fake_bundle(self, refs_with_sizes):
        return self._FakeBundle(
            [
                (
                    ref,
                    BlockMetadata(
                        num_rows=1, size_bytes=size, input_files=None, exec_stats=None
                    ),
                )
                for ref, size in refs_with_sizes
            ]
        )

    def _make_op(self):
        input_op = InputDataBuffer(DataContext.get_current(), [])
        upstream = InputDataBuffer(DataContext.get_current(), [])
        op = AllToAllOperator(
            bulk_fn=MagicMock(),
            input_op=input_op,
            data_context=DataContext.get_current(),
        )
        counter = BlockRefCounter()
        op._block_ref_counter = counter
        op._metrics = MagicMock()
        op.start(ExecutionOptions())
        return op, upstream, counter

    def test_passthrough_bulk_fn_does_not_mutate_counter(self):
        """A bulk_fn that returns the same ObjectRefs (e.g. randomize_blocks)
        must not crash or alter the counter — blocks stay tracked under their
        original owners.
        """
        op, upstream, counter = self._make_op()

        ref1, ref2 = object(), object()
        counter.on_block_produced(ref1, 100, upstream)
        counter.on_block_produced(ref2, 200, upstream)

        b1 = self._fake_bundle([(ref1, 100)])
        b2 = self._fake_bundle([(ref2, 200)])

        # Simulate passthrough: bulk_fn returns the same refs in different order.
        op._bulk_fn = lambda refs, ctx: ([b2, b1], {})
        op._input_buffer.add(b1)
        op._input_buffer.add(b2)
        op.all_inputs_done()

        # No crash, and both refs still tracked under their original owner.
        assert counter.get_object_store_memory_usage(upstream) == 300
        assert counter.get_object_store_memory_usage(op) == 0

    def test_materializing_bulk_fn_transfers_attribution(self):
        """A bulk_fn that produces new ObjectRefs (e.g. sort) must register
        the new refs under AllToAllOperator and untrack the consumed inputs.
        """
        op, upstream, counter = self._make_op()

        in_ref = object()
        counter.on_block_produced(in_ref, 100, upstream)
        in_bundle = self._fake_bundle([(in_ref, 100)])

        out_ref = object()
        out_bundle = self._fake_bundle([(out_ref, 250)])

        op._bulk_fn = lambda refs, ctx: ([out_bundle], {})
        op._input_buffer.add(in_bundle)
        op.all_inputs_done()

        # Input consumed, output registered under op.
        assert counter.get_object_store_memory_usage(upstream) == 0
        assert counter.get_object_store_memory_usage(op) == 250


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
