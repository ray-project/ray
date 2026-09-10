import warnings
from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.execution import create_resource_allocator
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.union_operator import UnionOperator
from ray.data._internal.execution.resource_manager import (
    ResourceManager,
)
from ray.data._internal.execution.streaming_executor_state import (
    build_streaming_topology,
)
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.util.annotations import RayDeprecationWarning


class StubBlockRefCounter(BlockRefCounter):
    """Test double for BlockRefCounter with directly settable per-operator usage."""

    def __init__(self):
        super().__init__(add_object_out_of_scope_callback=lambda *_: True)

    def set_usage(self, producer_id: str, size_bytes: int) -> None:
        self._bytes_by_producer[producer_id] = size_bytes


def test_execution_options_deprecated_defaults_initialized_without_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", RayDeprecationWarning)
        options = ExecutionOptions()

    assert options.exclude_resources == ExecutionResources.zero()
    assert options.actor_locality_enabled is True


@pytest.mark.parametrize(
    ("attr", "value"),
    [
        ("actor_locality_enabled", False),
        ("exclude_resources", ExecutionResources(cpu=1)),
    ],
)
def test_execution_options_emits_deprecation_warning(attr, value):
    options = ExecutionOptions()
    with pytest.warns(RayDeprecationWarning, match=rf"ExecutionOptions\.{attr}"):
        setattr(options, attr, value)


def test_execution_options_exclude_resources_none_normalized():
    options = ExecutionOptions()

    with pytest.warns(
        RayDeprecationWarning, match="ExecutionOptions\\.exclude_resources"
    ):
        options.exclude_resources = None

    assert options.exclude_resources == ExecutionResources.zero()


def test_execution_options_set_exclude_resources_internal_no_warning():
    options = ExecutionOptions()

    with warnings.catch_warnings():
        warnings.simplefilter("error", RayDeprecationWarning)
        options._set_exclude_resources(ExecutionResources(cpu=1))

    assert options.exclude_resources == ExecutionResources(cpu=1)


def test_physical_operator_tracks_output_dependencies():
    input_op = PhysicalOperator("input", [], DataContext.get_current())
    downstream_op = PhysicalOperator(
        "downstream", [input_op], DataContext.get_current()
    )

    assert input_op.output_dependencies == [downstream_op]


def test_physical_apply_transform_rewires_all_input_output_dependencies():
    ctx = DataContext.get_current()
    left_input = PhysicalOperator("left_input", [], ctx)
    right_input = PhysicalOperator("right_input", [], ctx)
    root = PhysicalOperator("root", [left_input, right_input], ctx)
    left_replacement = PhysicalOperator("left_replacement", [], ctx)

    transformed_root = root._apply_transform(
        lambda op: left_replacement if op is left_input else op
    )

    assert transformed_root is not root
    assert transformed_root.id != root.id
    assert transformed_root.metrics is not root.metrics
    assert transformed_root.input_dependencies == [left_replacement, right_input]
    assert transformed_root in left_replacement.output_dependencies
    assert transformed_root in right_input.output_dependencies
    assert root not in left_input.output_dependencies
    assert root not in right_input.output_dependencies


def test_physical_apply_transform_rewires_when_current_node_is_replaced():
    ctx = DataContext.get_current()
    left_input = PhysicalOperator("left_input", [], ctx)
    right_input = PhysicalOperator("right_input", [], ctx)
    root = PhysicalOperator("root", [left_input, right_input], ctx)

    transformed_root = root._apply_transform(
        lambda op: PhysicalOperator("replacement", [left_input], ctx)
        if op is root
        else op
    )

    assert transformed_root is not root
    assert transformed_root in left_input.output_dependencies
    assert root not in left_input.output_dependencies
    assert root not in right_input.output_dependencies
    assert transformed_root not in right_input.output_dependencies


def test_physical_apply_transform_deep_chain_no_stale_downstream_refs():
    ctx = DataContext.get_current()
    leaf = PhysicalOperator("leaf", [], ctx)
    mid = PhysicalOperator("mid", [leaf], ctx)
    root = PhysicalOperator("root", [mid], ctx)

    def transform(op: PhysicalOperator) -> PhysicalOperator:
        if op is leaf:
            return PhysicalOperator("leaf_replacement", [], ctx)
        if op.name == "root":
            return PhysicalOperator("root_replacement", op.input_dependencies, ctx)
        return op

    transformed_root = root._apply_transform(transform)
    transformed_mid = transformed_root.input_dependencies[0]
    transformed_leaf = transformed_mid.input_dependencies[0]

    assert transformed_root.name == "root_replacement"
    assert transformed_mid is not mid
    assert transformed_leaf.name == "leaf_replacement"
    assert root not in transformed_mid.output_dependencies
    assert transformed_mid.output_dependencies == [transformed_root]


def test_physical_apply_transform_rejects_in_place_input_mutation():
    ctx = DataContext.get_current()
    old_input = PhysicalOperator("old_input", [], ctx)
    new_input = PhysicalOperator("new_input", [], ctx)
    root = PhysicalOperator("root", [old_input], ctx)

    def transform(op: PhysicalOperator) -> PhysicalOperator:
        if op is root:
            op._input_dependencies = [new_input]
            return op
        return op

    with pytest.raises(
        AssertionError,
        match="In-place input mutation is not supported; return a new node instead.",
    ):
        root._apply_transform(transform)


def test_union_memory_attribution_outqueue():
    """Test that Union's external output queue memory is attributed per-producer
    to upstream operators, avoiding double-counting.

    When Union is ineligible (throttling_disabled=True), its memory is rolled
    into upstream operators via _get_downstream_ineligible_ops_usage. With
    per-producer tracking on BlockRefCounter, each upstream only gets
    charged for the bytes it contributed to Union's ext output queue.

    Regression test for https://github.com/ray-project/ray/pull/61040.
    """
    # Topology:
    #   input1 ───┐
    #             ├─▶ union_op
    #   input2 ───┘
    input1 = PhysicalOperator("op1", [], DataContext.get_current())
    input2 = PhysicalOperator("op2", [], DataContext.get_current())
    union_op = UnionOperator(DataContext.get_current(), input1, input2)
    counter = StubBlockRefCounter()
    topology = build_streaming_topology(union_op, ExecutionOptions(), counter)

    total_resources = ExecutionResources(cpu=0, object_store_memory=200)
    resource_manager = ResourceManager(
        topology,
        ExecutionOptions(),
        lambda: total_resources,
        DataContext.get_current(),
        counter,
    )

    # Create RefBundles tagged with producer via BlockRefCounter.
    block_ref1 = ray.ObjectRef(b"1" * 28)
    block_ref2 = ray.ObjectRef(b"2" * 28)
    meta1 = BlockMetadata(num_rows=1, size_bytes=10, input_files=None, exec_stats=None)
    meta2 = BlockMetadata(num_rows=1, size_bytes=30, input_files=None, exec_stats=None)
    bundle1 = RefBundle(
        [BlockEntry(block_ref1, meta1)],
        owns_blocks=True,
        schema=None,
    )
    bundle2 = RefBundle(
        [BlockEntry(block_ref2, meta2)],
        owns_blocks=True,
        schema=None,
    )

    # Add to Union's ext output queue (simulating process_completed_tasks drain).
    topology[union_op].add_output(bundle1)
    topology[union_op].add_output(bundle2)
    counter.on_block_produced(block_ref1, 10, input1.id)
    counter.on_block_produced(block_ref2, 30, input2.id)
    resource_manager.update_usages()

    # Union is ineligible, so its memory is attributed to upstream ops.
    # input1 should only be charged for bundle1 (10 bytes).
    # input2 should only be charged for bundle2 (30 bytes).
    input1_usage = resource_manager.get_op_usage(
        input1, include_ineligible_downstream=True
    ).object_store_memory
    input2_usage = resource_manager.get_op_usage(
        input2, include_ineligible_downstream=True
    ).object_store_memory

    assert input1_usage == 10, f"Expected 10, got {input1_usage}"
    assert input2_usage == 30, f"Expected 30, got {input2_usage}"

    # Total should be 40, not 80 (which would happen with double-counting).
    total = input1_usage + input2_usage
    assert total == 40, f"Expected 40, got {total}"


def test_union_memory_attribution_internal_inqueue():
    """Test that per-input attribution correctly charges each upstream operator
    only for the blocks it produced in the union's internal input queue.

    When preserve_order=True, the union operator buffers blocks per-input.
    The resource manager should attribute each input buffer's memory only to
    the corresponding upstream operator, not to all upstream operators.
    """
    # Create a mock topology:
    #
    #   input1 ───┐
    #             ├─▶ union_op
    #   input2 ───┘
    input1 = PhysicalOperator("op1", [], DataContext.get_current())
    input2 = PhysicalOperator("op2", [], DataContext.get_current())
    union_op = UnionOperator(DataContext.get_current(), input1, input2)

    options = ExecutionOptions()
    options.preserve_order = True
    counter = StubBlockRefCounter()
    topology = build_streaming_topology(union_op, options, counter)

    # Create a resource manager.
    total_resources = ExecutionResources(cpu=0, object_store_memory=200)
    resource_manager = ResourceManager(
        topology,
        options,
        lambda: total_resources,
        DataContext.get_current(),
        counter,
    )

    # Create two 10-byte RefBundles with distinct block refs (simulates real execution
    # where each block from a source has its own ObjectRef).
    block_ref1 = ray.ObjectRef(b"1" * 28)
    block_ref2 = ray.ObjectRef(b"2" * 28)
    block_metadata = BlockMetadata(
        num_rows=1, size_bytes=10, input_files=None, exec_stats=None
    )
    bundle1 = RefBundle(
        [BlockEntry(block_ref1, block_metadata)], owns_blocks=True, schema=None
    )
    bundle2 = RefBundle(
        [BlockEntry(block_ref2, block_metadata)], owns_blocks=True, schema=None
    )

    # Add blocks only to input2's buffer inside the union operator.
    # With preserve_order=True, _add_input_inner routes to _input_buffers[input_index].
    union_op.add_input(bundle1, input_index=1)
    union_op.add_input(bundle2, input_index=1)
    # Blocks in union's input buffer are attributed to their producer (input2).
    counter.on_block_produced(block_ref1, 10, input2.id)
    counter.on_block_produced(block_ref2, 10, input2.id)

    resource_manager.update_usages()

    # input2 should be charged for its blocks in the union's input buffer (20 bytes).
    input2_usage = resource_manager.get_op_usage(
        input2, include_ineligible_downstream=True
    ).object_store_memory
    # input1 should NOT be charged for input2's blocks (0 bytes from union inqueue).
    input1_usage = resource_manager.get_op_usage(
        input1, include_ineligible_downstream=True
    ).object_store_memory

    assert input1_usage == 0
    assert input2_usage == 20


def test_union_memory_attribution_through_limit():
    """Test that producer attribution survives through other ineligible operators
    (e.g., Limit) so that per-producer memory attribution works for longer
    ineligible chains.

    Topology:
        input1 -> limit1 ---+
                             +---> union_op ---> limit_out
        input2 -> limit2 ---+

    Simulates a steady-state snapshot where bundles accumulate at multiple
    queues of operators simultaneously, matching real execution behavior.
    """
    input1 = PhysicalOperator("op1", [], DataContext.get_current())
    limit1 = LimitOperator(100, input1, DataContext.get_current())
    input2 = PhysicalOperator("op2", [], DataContext.get_current())
    limit2 = LimitOperator(100, input2, DataContext.get_current())
    union_op = UnionOperator(DataContext.get_current(), limit1, limit2)
    limit_out = LimitOperator(100, union_op, DataContext.get_current())
    counter = StubBlockRefCounter()
    topology = build_streaming_topology(limit_out, ExecutionOptions(), counter)

    total_resources = ExecutionResources(cpu=0, object_store_memory=1000)
    resource_manager = ResourceManager(
        topology,
        ExecutionOptions(),
        lambda: total_resources,
        DataContext.get_current(),
        counter,
    )

    def make_bundle(ref_byte, size_bytes):
        block_ref = ray.ObjectRef(ref_byte * 28)
        meta = BlockMetadata(
            num_rows=1, size_bytes=size_bytes, input_files=None, exec_stats=None
        )
        return block_ref, RefBundle(
            [BlockEntry(block_ref, meta)],
            owns_blocks=True,
            schema=None,
        )

    def get_attributed_usage(op):
        return resource_manager.get_op_usage(
            op, include_ineligible_downstream=True
        ).object_store_memory

    # Place bundles at different points in the ineligible chain,
    # simulating a steady-state snapshot:
    #
    # limit1 outqueue:    [B_a(10MB, input1.id)]  <- from input1
    # union_op outqueue:  [B_b(10MB, input1.id)]  <- from input1
    # limit_out outqueue: [B_c(30MB, input2.id)]  <- from input2

    # Bundle waiting in limit1's outqueue (tagged with input1's id).
    ref_a, bundle_a = make_bundle(b"a", 10)
    topology[limit1].add_output(bundle_a)
    counter.on_block_produced(ref_a, 10, input1.id)

    # Bundle in Union's outqueue, tagged with input1's id.
    ref_b, bundle_b = make_bundle(b"b", 10)
    topology[union_op].add_output(bundle_b)
    counter.on_block_produced(ref_b, 10, input1.id)

    # Bundle in limit_out's outqueue, tagged with input2's id.
    ref_c, bundle_c = make_bundle(b"c", 30)
    topology[limit_out].add_output(bundle_c)
    counter.on_block_produced(ref_c, 30, input2.id)

    resource_manager.update_usages()

    # input1 should be charged for:
    #   - B_a in limit1 outqueue (10MB)
    #   - B_b in union_op outqueue (10MB)
    # Total for input1: 20MB
    assert get_attributed_usage(input1) == 20, get_attributed_usage(input1)

    # input2 should be charged for:
    #   - B_c in limit_out outqueue (30MB)
    # Total for input2: 30MB
    assert get_attributed_usage(input2) == 30, get_attributed_usage(input2)

    total = get_attributed_usage(input1) + get_attributed_usage(input2)
    assert total == 50, f"Expected 50, got {total}"


def test_union_operator_not_allocated_resources(restore_data_context):
    ctx = DataContext.get_current()
    ctx.op_resource_reservation_enabled = True
    input_op1 = PhysicalOperator("input1", [], ctx)
    input_op2 = PhysicalOperator("input2", [], ctx)
    union_op = UnionOperator(ctx, input_op1, input_op2)

    counter = StubBlockRefCounter()
    topo = build_streaming_topology(union_op, ExecutionOptions(), counter)
    resource_manager = ResourceManager(
        topo, ExecutionOptions(), MagicMock(), ctx, counter
    )
    allocator = create_resource_allocator(resource_manager, ctx)
    assert allocator is not None

    allocator.update_budgets(
        limits=ExecutionResources(cpu=1, gpu=0, object_store_memory=1000)
    )
    allocation = allocator.get_allocation(union_op)
    assert allocation is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
