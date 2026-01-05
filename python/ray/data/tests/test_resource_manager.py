import math
import time
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.join import JoinOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.union_operator import UnionOperator
from ray.data._internal.execution.resource_manager import (
    ResourceManager,
)
from ray.data._internal.execution.streaming_executor_state import (
    build_streaming_topology,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.context import MAX_SAFE_BLOCK_SIZE_FACTOR, DataContext
from ray.data.tests.conftest import *  # noqa


def mock_map_op(
    input_op: PhysicalOperator,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    compute_strategy: Optional[ComputeStrategy] = None,
    incremental_resource_usage: Optional[ExecutionResources] = None,
    name="Map",
):
    op = MapOperator.create(
        MagicMock(),
        input_op,
        DataContext.get_current(),
        ray_remote_args=ray_remote_args or {},
        compute_strategy=compute_strategy,
        name=name,
    )
    op.start(ExecutionOptions())
    if incremental_resource_usage is not None:
        op.incremental_resource_usage = MagicMock(
            return_value=incremental_resource_usage
        )
    return op


def mock_union_op(
    input_ops,
    incremental_resource_usage=None,
):
    op = UnionOperator(
        DataContext.get_current(),
        *input_ops,
    )
    op.start = MagicMock(side_effect=lambda _: None)
    if incremental_resource_usage is not None:
        op.incremental_resource_usage = MagicMock(
            return_value=incremental_resource_usage
        )
    return op


def mock_join_op(
    left_input_op,
    right_input_op,
    incremental_resource_usage=None,
):
    left_input_op._logical_operators = [(MagicMock())]
    right_input_op._logical_operators = [(MagicMock())]

    with patch(
        "ray.data._internal.execution.operators.hash_shuffle._get_total_cluster_resources"
    ) as mock:
        mock.return_value = ExecutionResources(cpu=1)

        op = JoinOperator(
            DataContext.get_current(),
            left_input_op,
            right_input_op,
            ("id",),
            ("id",),
            "inner",
            num_partitions=1,
            partition_size_hint=1,
        )

    op.start = MagicMock(side_effect=lambda _: None)
    if incremental_resource_usage is not None:
        op.incremental_resource_usage = MagicMock(
            return_value=incremental_resource_usage
        )
    return op


class TestResourceManager:
    """Unit tests for ResourceManager."""

    def test_global_limits(self):
        cluster_resources = {"CPU": 10, "GPU": 5, "object_store_memory": 1000}
        default_object_store_memory_limit = math.ceil(
            cluster_resources["object_store_memory"]
            * ResourceManager.DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION
        )

        def get_total_resources():
            return ExecutionResources.from_resource_dict(cluster_resources)

        # Test default resource limits.
        # When no resource limits are set, the resource limits should default to
        # the cluster resources for CPU/GPU, and
        # DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION of cluster object store memory.
        options = ExecutionOptions()
        resource_manager = ResourceManager(
            MagicMock(), options, get_total_resources, DataContext.get_current()
        )
        expected = ExecutionResources(
            cpu=cluster_resources["CPU"],
            gpu=cluster_resources["GPU"],
            object_store_memory=default_object_store_memory_limit,
        )
        assert resource_manager.get_global_limits() == expected

        # Test setting resource_limits
        options = ExecutionOptions()
        options.resource_limits = ExecutionResources(
            cpu=1, gpu=2, object_store_memory=100
        )
        resource_manager = ResourceManager(
            MagicMock(), options, get_total_resources, DataContext.get_current()
        )
        expected = ExecutionResources(
            cpu=1,
            gpu=2,
            object_store_memory=100,
        )
        assert resource_manager.get_global_limits() == expected

        # Test setting exclude_resources
        # The actual limit should be the default limit minus the excluded resources.
        options = ExecutionOptions()
        options.exclude_resources = ExecutionResources(
            cpu=1, gpu=2, object_store_memory=100
        )
        resource_manager = ResourceManager(
            MagicMock(), options, get_total_resources, DataContext.get_current()
        )
        expected = ExecutionResources(
            cpu=cluster_resources["CPU"] - 1,
            gpu=cluster_resources["GPU"] - 2,
            object_store_memory=default_object_store_memory_limit - 100,
        )
        assert resource_manager.get_global_limits() == expected

        # Test that we don't support setting both resource_limits
        # and exclude_resources.
        with pytest.raises(ValueError):
            options = ExecutionOptions()
            options.resource_limits = ExecutionResources(cpu=2)
            options.exclude_resources = ExecutionResources(cpu=1)
            options.validate()

    def test_global_limits_cache(self):
        get_total_resources = MagicMock(return_value=ExecutionResources(4, 1, 0))

        cache_interval_s = 0.1
        with patch.object(
            ResourceManager,
            "GLOBAL_LIMITS_UPDATE_INTERVAL_S",
            cache_interval_s,
        ):
            resource_manager = ResourceManager(
                MagicMock(),
                ExecutionOptions(),
                get_total_resources,
                DataContext.get_current(),
            )
            expected_resource = ExecutionResources(4, 1, 0)
            # The first call should call ray.cluster_resources().
            assert resource_manager.get_global_limits() == expected_resource
            assert get_total_resources.call_count == 1
            # The second call should return the cached value.
            assert resource_manager.get_global_limits() == expected_resource
            assert get_total_resources.call_count == 1
            time.sleep(cache_interval_s)
            # After the cache interval, the third call should call
            # ray.cluster_resources() again.
            assert resource_manager.get_global_limits() == expected_resource
            assert get_total_resources.call_count == 2

    def test_update_usage(self):
        """Test calculating op_usage."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)
        topo = build_streaming_topology(o3, ExecutionOptions())

        # Mock different metrics that contribute to the resource usage.
        mock_cpu = {
            o1: 0,
            o2: 5,
            o3: 8,
        }
        mock_pending_task_outputs = {
            o1: 0,
            o2: 100,
            o3: 200,
        }
        mock_internal_outqueue = {
            o1: 0,
            o2: 300,
            o3: 400,
        }
        mock_external_outqueue_sizes = {
            o1: 100,
            o2: 500,
            o3: 600,
        }
        mock_internal_inqueue = {
            o1: 0,
            o2: 700,
            o3: 800,
        }
        mock_pending_task_inputs = {
            o1: 0,
            o2: 900,
            o3: 1000,
        }

        for op in [o1, o2, o3]:
            op.update_resource_usage = MagicMock()
            op.current_processor_usage = MagicMock(
                return_value=ExecutionResources(cpu=mock_cpu[op], gpu=0)
            )
            op.running_processor_usage = MagicMock(
                return_value=ExecutionResources(cpu=mock_cpu[op], gpu=0)
            )
            op.pending_processor_usage = MagicMock(
                return_value=ExecutionResources.zero()
            )
            op.extra_resource_usage = MagicMock(return_value=ExecutionResources.zero())
            op._metrics = MagicMock(
                obj_store_mem_pending_task_outputs=mock_pending_task_outputs[op],
                obj_store_mem_internal_outqueue=mock_internal_outqueue[op],
                obj_store_mem_internal_inqueue=mock_internal_inqueue[op],
                obj_store_mem_pending_task_inputs=mock_pending_task_inputs[op],
            )
            ref_bundle = MagicMock(
                size_bytes=MagicMock(return_value=mock_external_outqueue_sizes[op])
            )
            topo[op].add_output(ref_bundle)

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        resource_manager._op_resource_allocator = None
        resource_manager.update_usages()

        global_cpu = 0
        global_mem = 0
        for op in [o1, o2, o3]:
            if op == o1:
                # Resource usage of InputDataBuffer doesn't count.
                expected_mem = 0
            else:
                expected_mem = (
                    mock_pending_task_outputs[op]
                    + mock_internal_outqueue[op]
                    + mock_external_outqueue_sizes[op]
                )
                for next_op in op.output_dependencies:
                    expected_mem += (
                        +mock_internal_inqueue[next_op]
                        + mock_pending_task_inputs[next_op]
                    )
            op_usage = resource_manager.get_op_usage(op)
            assert op_usage.cpu == mock_cpu[op]
            assert op_usage.gpu == 0
            assert op_usage.object_store_memory == expected_mem
            if op != o1:
                assert (
                    resource_manager._mem_op_internal[op]
                    == mock_pending_task_outputs[op] + mock_internal_outqueue[op]
                )
                assert (
                    resource_manager._mem_op_outputs[op]
                    == expected_mem - resource_manager._mem_op_internal[op]
                )
            global_cpu += mock_cpu[op]
            global_mem += expected_mem

        assert resource_manager.get_global_usage() == ExecutionResources(
            global_cpu, 0, global_mem
        )

    def test_object_store_usage(self, restore_data_context):
        input = make_ref_bundles([[x] for x in range(1)])[0]
        input.size_bytes = MagicMock(return_value=1)

        o1 = InputDataBuffer(DataContext.get_current(), [input])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        topo = build_streaming_topology(o3, ExecutionOptions())
        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(return_value=ExecutionResources.zero()),
            DataContext.get_current(),
        )
        ray.data.DataContext.get_current()._max_num_blocks_in_streaming_gen_buffer = 1
        ray.data.DataContext.get_current().target_max_block_size = 2

        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 0
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # Objects in an operator's internal inqueue typically count toward the previous
        # operator's object store memory usage. However, data from an
        # `InputDataBuffer` aren't counted because they were created outside of this
        # execution.
        o2.metrics.on_input_queued(input)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 0
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # Operators estimate pending task outputs using the target max block size
        # multiplied by MAX_SAFE_BLOCK_SIZE_FACTOR (1.5) during no-sample phase.
        # In this case, the target max block size is 2, MAX_SAFE_BLOCK_SIZE_FACTOR
        # is 1.5, and there is at most 1 block in the streaming generator buffer,
        # so the estimated usage is 2 * 1.5 * 1 = 3.
        o2.metrics.on_input_dequeued(input)
        o2.metrics.on_task_submitted(0, input)
        resource_manager.update_usages()
        # target_max_block_size * factor * max_blocks
        expected_usage = 2 * MAX_SAFE_BLOCK_SIZE_FACTOR * 1
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        op2_usage = resource_manager.get_op_usage(o2).object_store_memory
        assert op2_usage == expected_usage
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # When the task finishes, we move the data from the streaming generator to the
        # operator's internal outqueue.
        o2.metrics.on_output_queued(input)
        o2.metrics.on_task_finished(0, None)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 1
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        o2.metrics.on_output_dequeued(input)
        topo[o2].output_queue.append(input)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 1
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # Objects in the current operator's internal inqueue count towards the previous
        # operator's object store memory usage.
        o3.metrics.on_input_queued(topo[o2].output_queue.pop())
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 1
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # Task inputs count toward the previous operator's object store memory
        # usage, and task outputs count toward the current operator's object
        # store memory usage. During no-sample phase, pending outputs are
        # estimated using target_max_block_size * MAX_SAFE_BLOCK_SIZE_FACTOR.
        o3.metrics.on_input_dequeued(input)
        o3.metrics.on_task_submitted(0, input)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 1
        # target_max_block_size (2) * factor (1.5) * max_blocks (1) = 3
        expected_o3_usage = 2 * MAX_SAFE_BLOCK_SIZE_FACTOR * 1
        op3_usage = resource_manager.get_op_usage(o3).object_store_memory
        assert op3_usage == expected_o3_usage

        # Task inputs no longer count once the task is finished.
        o3.metrics.on_output_queued(input)
        o3.metrics.on_task_finished(0, None)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 0
        assert resource_manager.get_op_usage(o3).object_store_memory == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
