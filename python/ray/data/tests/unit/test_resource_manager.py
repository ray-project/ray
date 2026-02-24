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

    # Create a 10-byte RefBundle.
    block_ref = ray.ObjectRef(b"1" * 28)
    block_metadata = BlockMetadata(
        num_rows=1, size_bytes=10, input_files=None, exec_stats=None
    )
    bundle = RefBundle([(block_ref, block_metadata)], owns_blocks=True, schema=None)

    # Add blocks only to input2's buffer inside the union operator.
    # With preserve_order=True, _add_input_inner routes to _input_buffers[input_index].
    union_op.add_input(bundle, input_index=1)
    union_op.add_input(bundle, input_index=1)

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
