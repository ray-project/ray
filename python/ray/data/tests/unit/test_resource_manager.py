import pytest

import ray
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
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


def test_does_not_double_count_usage_from_union():
    """Regression test for https://github.com/ray-project/ray/pull/61040."""
    # Create a mock topology:
    #
    #   input1 ───┐
    #             ├─▶ union_op
    #   input2 ───┘
    input1 = PhysicalOperator("op1", [], DataContext.get_current())
    input2 = PhysicalOperator("op2", [], DataContext.get_current())
    union_op = UnionOperator(DataContext.get_current(), input1, input2)
    topology = build_streaming_topology(union_op, ExecutionOptions())

    # Create a resource manager.
    total_resources = ExecutionResources(cpu=0, object_store_memory=2)
    resource_manager = ResourceManager(
        topology, ExecutionOptions(), lambda: total_resources, DataContext.get_current()
    )

    # Create a 1-byte `RefBundle`.
    block_ref = ray.ObjectRef(b"1" * 28)
    block_metadata = BlockMetadata(
        num_rows=1, size_bytes=1, input_files=None, exec_stats=None
    )
    bundle = RefBundle([(block_ref, block_metadata)], owns_blocks=True, schema=None)

    # Add two 1-byte `RefBundle` to the union operator.
    topology[union_op].add_output(bundle)
    topology[union_op].add_output(bundle)
    resource_manager.update_usages()

    # The total object store memory usage should be 2. If the resource manager double-
    # counts the usage from the union operator, the total object store memory usage can
    # be greater than 2.
    total_object_store_memory = sum(
        [
            resource_manager.get_op_usage(
                op, include_ineligible_downstream=True
            ).object_store_memory
            for op in topology.keys()
        ]
    )
    assert total_object_store_memory == 2, total_object_store_memory


def test_per_input_inqueue_attribution_for_union():
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
    topology = build_streaming_topology(union_op, options)

    # Create a resource manager.
    total_resources = ExecutionResources(cpu=0, object_store_memory=200)
    resource_manager = ResourceManager(
        topology, options, lambda: total_resources, DataContext.get_current()
    )

    # Create two 10-byte RefBundles with distinct block refs (simulates real execution
    # where each block from a source has its own ObjectRef).
    block_ref1 = ray.ObjectRef(b"1" * 28)
    block_ref2 = ray.ObjectRef(b"2" * 28)
    block_metadata = BlockMetadata(
        num_rows=1, size_bytes=10, input_files=None, exec_stats=None
    )
    bundle1 = RefBundle([(block_ref1, block_metadata)], owns_blocks=True, schema=None)
    bundle2 = RefBundle([(block_ref2, block_metadata)], owns_blocks=True, schema=None)

    # Add blocks only to input2's buffer inside the union operator.
    # With preserve_order=True, _add_input_inner routes to _input_buffers[input_index].
    union_op.add_input(bundle1, input_index=1)
    union_op.add_input(bundle2, input_index=1)

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
