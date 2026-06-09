import math
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, call, patch

import pyarrow as pa
import pytest

import ray
from ray.data import DataContext, ExecutionResources
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.hash_aggregate import HashAggregateOperator
from ray.data._internal.execution.operators.hash_shuffle import (
    HashShuffleOperator,
    _PartitionStats,
)
from ray.data._internal.execution.operators.join import JoinOperator
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import JoinType
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.util import GiB, MiB
from ray.data.aggregate import AggregateFnV2, Count, Sum
from ray.data.block import BlockAccessor, BlockMetadata


def _create_aggregator_pool_for_test(op, estimated_dataset_bytes: Optional[int]):
    pool = op._create_aggregator_pool(
        estimated_dataset_bytes=estimated_dataset_bytes,
    )
    op._aggregator_pool = pool
    return pool


def _make_ref_bundle(size_bytes: int) -> RefBundle:
    block = pa.table({"id": [1]})
    return RefBundle(
        [
            BlockEntry(
                ray.put(block),
                BlockMetadata(
                    num_rows=1,
                    size_bytes=size_bytes,
                    exec_stats=None,
                    input_files=None,
                ),
            )
        ],
        schema=block.schema,
        owns_blocks=False,
    )


@dataclass
class JoinTestCase:
    # Expected outputs
    expected_ray_remote_args: Dict[str, Any]
    expected_num_partitions: int
    expected_num_aggregators: int

    # Input dataset configurations
    left_size_bytes: Optional[int]
    right_size_bytes: Optional[int]
    left_num_blocks: Optional[int]
    right_num_blocks: Optional[int]

    # Join configuration
    target_num_partitions: Optional[int]

    # Cluster resources (for testing different resource scenarios)
    total_cpu: float = 4.0
    total_memory: int = 32 * GiB


@pytest.mark.parametrize(
    "tc",
    [
        # Case 1: Auto-derived partitions with limited CPUs
        JoinTestCase(
            left_size_bytes=1 * GiB,
            right_size_bytes=2 * GiB,
            left_num_blocks=10,
            right_num_blocks=5,
            target_num_partitions=None,  # Auto-derive
            total_cpu=4.0,
            expected_num_partitions=10,  # max(10, 5)
            expected_num_aggregators=4,  # min(10 partitions, 4 CPUs) = 4
            expected_ray_remote_args={
                "max_concurrency": 3,  # ceil(10 partitions / 4 aggregators)
                "num_cpus": 0.25,  # 4 CPUs * 25% / 4 aggregators
                "memory": 1771674012,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 2: Single partition (much higher memory overhead)
        JoinTestCase(
            left_size_bytes=1 * GiB,
            right_size_bytes=1 * GiB,
            left_num_blocks=10,
            right_num_blocks=10,
            target_num_partitions=1,
            total_cpu=4.0,
            expected_num_partitions=1,
            expected_num_aggregators=1,  # min(1 partition, 4 CPUs) = 1
            expected_ray_remote_args={
                "max_concurrency": 1,
                "num_cpus": 1.0,  # 4 CPUs * 25% / 1 aggregator
                "memory": 8589934592,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 3: Limited CPU resources affecting num_cpus calculation
        JoinTestCase(
            left_size_bytes=2 * GiB,
            right_size_bytes=2 * GiB,
            left_num_blocks=20,
            right_num_blocks=20,
            target_num_partitions=40,
            total_cpu=2.0,  # Only 2 CPUs available
            expected_num_partitions=40,
            expected_num_aggregators=2,  # min(40 partitions, 2 CPUs) = 2
            expected_ray_remote_args={
                "max_concurrency": 8,  # min(ceil(40/2), 8) = 8
                "num_cpus": 0.25,  # 2 CPUs * 25% / 2 aggregators
                "memory": 2469606197,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 4: Testing with many CPUs and partitions
        JoinTestCase(
            left_size_bytes=10 * GiB,
            right_size_bytes=10 * GiB,
            left_num_blocks=100,
            right_num_blocks=100,
            target_num_partitions=100,
            total_cpu=32.0,
            expected_num_partitions=100,
            expected_num_aggregators=32,  # min(100 partitions, 32 CPUs)
            expected_ray_remote_args={
                "max_concurrency": 4,  # ceil(100 / 32)
                "num_cpus": 0.25,  # 32 CPUs * 25% / 32 aggregators
                "memory": 1315333735,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 5: Testing max aggregators cap (128 default)
        JoinTestCase(
            left_size_bytes=50 * GiB,
            right_size_bytes=50 * GiB,
            left_num_blocks=200,
            right_num_blocks=200,
            target_num_partitions=200,
            total_cpu=256.0,  # Many CPUs
            expected_num_partitions=200,
            expected_num_aggregators=128,  # min(200, min(256, 128 (default max))
            expected_ray_remote_args={
                "max_concurrency": 2,  # ceil(200 / 128)
                "num_cpus": 0.5,  # 256 CPUs * 25% / 128 aggregators
                "memory": 2449473536,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 6: Testing num_cpus derived from memory allocation
        JoinTestCase(
            left_size_bytes=50 * GiB,
            right_size_bytes=50 * GiB,
            left_num_blocks=200,
            right_num_blocks=200,
            target_num_partitions=None,
            total_cpu=1024,  # Many CPUs
            expected_num_partitions=200,
            expected_num_aggregators=128,  # min(200, min(1000, 128 (default max))
            expected_ray_remote_args={
                "max_concurrency": 2,  # ceil(200 / 128)
                "num_cpus": 0.57,  # ~2.5Gb / 4Gb = ~0.57
                "memory": 2449473536,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 7: No dataset size estimates available (fallback to default memory request)
        # Memory calculation (fallback):
        #   max_mem_per_agg = 32 GiB / 32 = 1 GiB
        #   modest_mem = 1 GiB / 2 = 512 MiB
        #   memory = min(512 MiB, DEFAULT_1GiB) = 512 MiB = 536870912
        # CPU calculation:
        #   cap = min(4.0, 32.0 * 0.25 / 32) = 0.25
        #   target = min(0.25, 536870912 / 4 GiB) = 0.12
        JoinTestCase(
            left_size_bytes=None,
            right_size_bytes=None,
            left_num_blocks=None,
            right_num_blocks=None,
            target_num_partitions=None,
            total_cpu=32,
            expected_num_partitions=200,  # default parallelism
            expected_num_aggregators=32,  # min(200, min(1000, 128 (default max))
            expected_ray_remote_args={
                "max_concurrency": 7,  # ceil(200 / 32)
                "num_cpus": 0.12,
                "memory": 536870912,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
    ],
)
def test_join_aggregator_remote_args(
    ray_start_regular,
    tc,
):
    """Test that join operator correctly estimates memory, CPU, and other resources
    for Aggregator actors based on dataset size estimates as well as cluster resources.
    """

    left_logical_op_mock = MagicMock(LogicalOperator)
    left_logical_op_mock.infer_metadata.return_value = BlockMetadata(
        num_rows=None,
        size_bytes=tc.left_size_bytes,
        exec_stats=None,
        input_files=None,
    )
    left_logical_op_mock.estimated_num_outputs.return_value = tc.left_num_blocks

    left_op_mock = MagicMock(PhysicalOperator)
    left_op_mock._output_dependencies = []
    left_op_mock._logical_operators = [left_logical_op_mock]
    left_op_mock.num_output_splits.return_value = 1

    right_logical_op_mock = MagicMock(LogicalOperator)
    right_logical_op_mock.infer_metadata.return_value = BlockMetadata(
        num_rows=None,
        size_bytes=tc.right_size_bytes,
        exec_stats=None,
        input_files=None,
    )
    right_logical_op_mock.estimated_num_outputs.return_value = tc.right_num_blocks

    right_op_mock = MagicMock(PhysicalOperator)
    right_op_mock._output_dependencies = []
    right_op_mock._logical_operators = [right_logical_op_mock]
    right_op_mock.num_output_splits.return_value = 1

    # Patch the total cluster resources
    with patch(
        "ray.data._internal.execution.operators.hash_shuffle.ray.cluster_resources",
        return_value={"CPU": tc.total_cpu, "memory": tc.total_memory},
    ):
        # Create the join operator
        op = JoinOperator(
            left_input_op=left_op_mock,
            right_input_op=right_op_mock,
            data_context=DataContext.get_current(),
            left_key_columns=("id",),
            right_key_columns=("id",),
            join_type=JoinType.INNER,
            num_partitions=tc.target_num_partitions,
        )

        # Validate the estimations
        assert op._num_partitions == tc.expected_num_partitions
        assert op._aggregator_pool is None

        estimated_dataset_bytes = (
            tc.left_size_bytes + tc.right_size_bytes
            if tc.left_size_bytes is not None and tc.right_size_bytes is not None
            else None
        )
        pool = _create_aggregator_pool_for_test(op, estimated_dataset_bytes)

        assert pool.num_aggregators == tc.expected_num_aggregators
        assert pool._aggregator_ray_remote_args == tc.expected_ray_remote_args


@dataclass
class HashOperatorTestCase:
    # Expected outputs
    expected_ray_remote_args: Dict[str, Any]
    expected_num_partitions: int
    expected_num_aggregators: int
    # Input dataset configuration
    input_size_bytes: Optional[int]
    input_num_blocks: Optional[int]
    # Operator configuration
    target_num_partitions: Optional[int]
    # Cluster resources (for testing different resource scenarios)
    total_cpu: float = 4.0
    total_memory: int = 32 * GiB


@pytest.mark.parametrize(
    "tc",
    [
        # Case 1: Auto-derived partitions with limited CPUs
        HashOperatorTestCase(
            input_size_bytes=2 * GiB,
            input_num_blocks=16,
            target_num_partitions=None,
            total_cpu=4.0,
            expected_num_partitions=16,
            expected_num_aggregators=4,
            expected_ray_remote_args={
                "max_concurrency": 4,
                "num_cpus": 0.16,
                "memory": 671088640,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 2: Single partition produced
        HashOperatorTestCase(
            input_size_bytes=512 * MiB,
            input_num_blocks=8,
            target_num_partitions=1,
            total_cpu=8.0,
            expected_num_partitions=1,
            expected_num_aggregators=1,
            expected_ray_remote_args={
                "max_concurrency": 1,
                "num_cpus": 0.25,
                "memory": 1073741824,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 3: Many CPUs
        HashOperatorTestCase(
            input_size_bytes=16 * GiB,
            input_num_blocks=128,
            target_num_partitions=32,
            total_cpu=256.0,
            expected_num_partitions=32,
            expected_num_aggregators=32,
            expected_ray_remote_args={
                "max_concurrency": 1,
                "num_cpus": 0.25,
                "memory": 1073741824,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 4: Testing num_cpus derived from memory allocation
        HashOperatorTestCase(
            input_size_bytes=50 * GiB,
            input_num_blocks=200,
            target_num_partitions=None,
            total_cpu=1024,  # Many CPUs
            expected_num_partitions=200,
            expected_num_aggregators=128,  # min(200, min(1000, 128 (default max))
            expected_ray_remote_args={
                "max_concurrency": 2,  # ceil(200 / 128)
                "num_cpus": 0.16,  # ~0.6Gb / 4Gb = ~0.16
                "memory": 687865856,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 6: No dataset size estimate inferred (fallback to default memory request)
        # Memory calculation (fallback):
        #   max_mem_per_agg = 32 GiB / 32 = 1 GiB
        #   modest_mem = 1 GiB / 2 = 512 MiB
        #   memory = min(512 MiB, DEFAULT_1GiB) = 512 MiB = 536870912
        # CPU calculation:
        #   cap = min(4.0, 32.0 * 0.25 / 32) = 0.25
        #   target = min(0.25, 536870912 / 4 GiB) = 0.12
        HashOperatorTestCase(
            input_size_bytes=None,
            input_num_blocks=None,
            target_num_partitions=None,
            total_cpu=32.0,
            expected_num_partitions=200,
            expected_num_aggregators=32,
            expected_ray_remote_args={
                "max_concurrency": 7,
                "num_cpus": 0.12,
                "memory": 536870912,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
    ],
)
def test_hash_aggregate_operator_remote_args(
    ray_start_regular,
    tc,
):
    """Test that HashAggregateOperator correctly estimates memory, CPU, and other resources
    for aggregator actors based on dataset size estimates as well as cluster resources.
    """
    logical_op_mock = MagicMock(LogicalOperator)
    logical_op_mock.infer_metadata.return_value = BlockMetadata(
        num_rows=None,
        size_bytes=tc.input_size_bytes,
        exec_stats=None,
        input_files=None,
    )
    logical_op_mock.estimated_num_outputs.return_value = tc.input_num_blocks

    op_mock = MagicMock(PhysicalOperator)
    op_mock._output_dependencies = []
    op_mock._logical_operators = [logical_op_mock]
    op_mock.num_output_splits.return_value = 1

    # Create some test aggregation functions
    agg_fns = [Sum("value"), Count()]

    # Patch the total cluster resources
    with patch(
        "ray.data._internal.execution.operators.hash_shuffle.ray.cluster_resources",
        return_value={"CPU": tc.total_cpu, "memory": tc.total_memory},
    ):
        # Create the hash aggregate operator
        op = HashAggregateOperator(
            input_op=op_mock,
            data_context=DataContext.get_current(),
            aggregation_fns=agg_fns,
            key_columns=("id",),
            num_partitions=tc.target_num_partitions,
        )

        # Validate the estimations
        assert op._num_partitions == tc.expected_num_partitions
        assert op._aggregator_pool is None

        pool = _create_aggregator_pool_for_test(op, tc.input_size_bytes)

        assert pool.num_aggregators == tc.expected_num_aggregators
        assert pool._aggregator_ray_remote_args == tc.expected_ray_remote_args


@pytest.mark.parametrize(
    "tc",
    [
        # Case 1: Auto-derived partitions with limited CPUs
        # Memory calculation:
        #   max_partitions_per_agg = ceil(16 / 4) = 4
        #   partition_size = ceil(2 GiB / 16) = 128 MiB
        #   shuffle + output = 2 * (128 MiB * 4) = 1024 MiB
        #   with 1.3x skew factor: ceil(1024 MiB * 1.3) = 1395864372
        # CPU calculation:
        #   cap = min(4.0, 4.0 * 0.25 / 4) = 0.25
        #   target = min(0.25, 1395864372 / 4 GiB) = 0.25
        HashOperatorTestCase(
            input_size_bytes=2 * GiB,
            input_num_blocks=16,
            target_num_partitions=None,
            total_cpu=4.0,
            expected_num_partitions=16,
            expected_num_aggregators=4,
            expected_ray_remote_args={
                "max_concurrency": 4,
                "num_cpus": 0.25,
                "memory": 1395864372,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 2: Single partition produced
        # Memory calculation:
        #   max_partitions_per_agg = ceil(1 / 1) = 1
        #   partition_size = ceil(512 MiB / 1) = 512 MiB
        #   shuffle + output = 2 * (512 MiB * 1) = 1024 MiB
        #   with 1.3x skew factor: ceil(1024 MiB * 1.3) = 1395864372
        # CPU calculation:
        #   cap = min(4.0, 8.0 * 0.25 / 1) = 2.0
        #   target = min(2.0, 1395864372 / 4 GiB) = 0.33
        HashOperatorTestCase(
            input_size_bytes=512 * MiB,
            input_num_blocks=8,
            target_num_partitions=1,
            total_cpu=8.0,
            expected_num_partitions=1,
            expected_num_aggregators=1,
            expected_ray_remote_args={
                "max_concurrency": 1,
                "num_cpus": 0.33,
                "memory": 1395864372,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 3: Many CPUs
        # Memory calculation:
        #   max_partitions_per_agg = ceil(32 / 32) = 1
        #   partition_size = ceil(16 GiB / 32) = 512 MiB
        #   shuffle + output = 2 * (512 MiB * 1) = 1024 MiB
        #   with 1.3x skew factor: ceil(1024 MiB * 1.3) = 1395864372
        # CPU calculation:
        #   cap = min(4.0, 256.0 * 0.25 / 32) = 2.0
        #   target = min(2.0, 1395864372 / 4 GiB) = 0.33
        HashOperatorTestCase(
            input_size_bytes=16 * GiB,
            input_num_blocks=128,
            target_num_partitions=32,
            total_cpu=256.0,
            expected_num_partitions=32,
            expected_num_aggregators=32,
            expected_ray_remote_args={
                "max_concurrency": 1,
                "num_cpus": 0.33,
                "memory": 1395864372,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 4: Testing num_cpus derived from memory allocation
        # Memory calculation:
        #   max_partitions_per_agg = ceil(200 / 128) = 2
        #   partition_size = ceil(50 GiB / 200) = 256 MiB
        #   shuffle + output = 2 * (256 MiB * 2) = 1024 MiB
        #   with 1.3x skew factor: ceil(1024 MiB * 1.3) = 1395864372
        # CPU calculation:
        #   cap = min(4.0, 1024 * 0.25 / 128) = 2.0
        #   target = min(2.0, 1395864372 / 4 GiB) = 0.33
        HashOperatorTestCase(
            input_size_bytes=50 * GiB,
            input_num_blocks=200,
            target_num_partitions=None,
            total_cpu=1024,  # Many CPUs
            expected_num_partitions=200,
            expected_num_aggregators=128,  # min(200, min(1000, 128 (default max))
            expected_ray_remote_args={
                "max_concurrency": 2,  # ceil(200 / 128)
                "num_cpus": 0.33,
                "memory": 1395864372,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
        # Case 5: No dataset size estimate inferred (fallback to default memory request)
        # Memory calculation (fallback):
        #   max_mem_per_agg = 32 GiB / 32 = 1 GiB
        #   modest_mem = 1 GiB / 2 = 512 MiB
        #   memory = min(512 MiB, DEFAULT_1GiB) = 512 MiB = 536870912
        # CPU calculation:
        #   cap = min(4.0, 32.0 * 0.25 / 32) = 0.25
        #   target = min(0.25, 536870912 / 4 GiB) = 0.12
        HashOperatorTestCase(
            input_size_bytes=None,
            input_num_blocks=None,
            target_num_partitions=None,
            total_cpu=32.0,
            expected_num_partitions=200,
            expected_num_aggregators=32,
            expected_ray_remote_args={
                "max_concurrency": 7,
                "num_cpus": 0.12,
                "memory": 536870912,
                "scheduling_strategy": "SPREAD",
                "allow_out_of_order_execution": True,
            },
        ),
    ],
)
def test_hash_shuffle_operator_remote_args(
    ray_start_regular,
    tc,
):
    """Test that HashShuffleOperator correctly estimates memory, CPU, and other resources
    for aggregator actors based on dataset size estimates as well as cluster resources.
    """
    logical_op_mock = MagicMock(LogicalOperator)
    logical_op_mock.infer_metadata.return_value = BlockMetadata(
        num_rows=None,
        size_bytes=tc.input_size_bytes,
        exec_stats=None,
        input_files=None,
    )
    logical_op_mock.estimated_num_outputs.return_value = tc.input_num_blocks

    op_mock = MagicMock(PhysicalOperator)
    op_mock._output_dependencies = []
    op_mock._logical_operators = [logical_op_mock]
    op_mock.num_output_splits.return_value = 1

    # Patch the total cluster resources
    with patch(
        "ray.data._internal.execution.operators.hash_shuffle.ray.cluster_resources",
        return_value={"CPU": tc.total_cpu, "memory": tc.total_memory},
    ):
        with patch(
            "ray.data._internal.execution.operators.hash_shuffle._get_total_cluster_resources"
        ) as mock_resources:
            mock_resources.return_value = ExecutionResources(
                cpu=tc.total_cpu, memory=tc.total_memory
            )

            # Create the hash shuffle operator
            op = HashShuffleOperator(
                input_op=op_mock,
                data_context=DataContext.get_current(),
                key_columns=("id",),
                num_partitions=tc.target_num_partitions,
            )

            # Validate the estimations
            assert op._num_partitions == tc.expected_num_partitions
            assert op._aggregator_pool is None

            pool = _create_aggregator_pool_for_test(op, tc.input_size_bytes)

            assert pool.num_aggregators == tc.expected_num_aggregators
            assert pool._aggregator_ray_remote_args == tc.expected_ray_remote_args


def test_aggregator_ray_remote_args_includes_context_label_selector(
    ray_start_regular, restore_data_context
):
    """ExecutionOptions.label_selector should appear on aggregator actor args."""
    DataContext.get_current().execution_options.label_selector = {"subcluster": "train"}

    logical_op_mock = MagicMock(LogicalOperator)
    logical_op_mock.infer_metadata.return_value = BlockMetadata(
        num_rows=None,
        size_bytes=2 * GiB,
        exec_stats=None,
        input_files=None,
    )
    logical_op_mock.estimated_num_outputs.return_value = 16

    op_mock = MagicMock(PhysicalOperator)
    op_mock._output_dependencies = []
    op_mock._logical_operators = [logical_op_mock]
    op_mock.num_output_splits.return_value = 1

    with patch(
        "ray.data._internal.execution.operators.hash_shuffle.ray.cluster_resources",
        return_value={"CPU": 4.0, "memory": 32 * GiB},
    ):
        op = HashAggregateOperator(
            input_op=op_mock,
            data_context=DataContext.get_current(),
            aggregation_fns=[Count()],
            key_columns=("id",),
        )

    pool = _create_aggregator_pool_for_test(op, 2 * GiB)

    assert pool._aggregator_ray_remote_args["label_selector"] == {"subcluster": "train"}


def test_aggregator_ray_remote_args_partial_override(ray_start_regular):
    """Test that partial override of aggregator_ray_remote_args retains default values.

    This tests the behavior where a user provides only some values (e.g., num_cpus)
    in aggregator_ray_remote_args_override, and the system should retain the default
    values for other parameters (e.g., scheduling_strategy, allow_out_of_order_execution).
    """
    logical_op_mock = MagicMock(LogicalOperator)
    logical_op_mock.infer_metadata.return_value = BlockMetadata(
        num_rows=None,
        size_bytes=2 * GiB,
        exec_stats=None,
        input_files=None,
    )
    logical_op_mock.estimated_num_outputs.return_value = 16

    op_mock = MagicMock(PhysicalOperator)
    op_mock._output_dependencies = []
    op_mock._logical_operators = [logical_op_mock]
    op_mock.num_output_splits.return_value = 1

    # Patch the total cluster resources
    with patch(
        "ray.data._internal.execution.operators.hash_shuffle.ray.cluster_resources",
        return_value={"CPU": 4.0, "memory": 32 * GiB},
    ):
        # Create operator with partial override (only num_cpus)
        op = HashAggregateOperator(
            input_op=op_mock,
            data_context=DataContext.get_current(),
            aggregation_fns=[Count()],
            key_columns=("id",),
            aggregator_ray_remote_args_override={
                "num_cpus": 0.5
            },  # Only override num_cpus
        )

        pool = _create_aggregator_pool_for_test(op, 2 * GiB)

        # Verify that num_cpus was overridden
        assert pool._aggregator_ray_remote_args["num_cpus"] == 0.5

        # Verify that default values are retained
        assert pool._aggregator_ray_remote_args["scheduling_strategy"] == "SPREAD"
        assert pool._aggregator_ray_remote_args["allow_out_of_order_execution"] is True

        # Verify that max_concurrency is still present
        assert "max_concurrency" in pool._aggregator_ray_remote_args

        # Verify that memory is still present
        assert "memory" in pool._aggregator_ray_remote_args


def test_hash_shuffle_does_not_infer_metadata_for_memory_during_construction(
    ray_start_regular,
):
    logical_op_mock = MagicMock(LogicalOperator)
    logical_op_mock.infer_metadata.side_effect = AssertionError(
        "logical metadata should not be used for hash-shuffle memory sizing"
    )
    logical_op_mock.estimated_num_outputs.return_value = 16

    op_mock = MagicMock(PhysicalOperator)
    op_mock._output_dependencies = []
    op_mock._logical_operators = [logical_op_mock]
    op_mock.num_output_splits.return_value = 1

    with patch(
        "ray.data._internal.execution.operators.hash_shuffle"
        "._get_total_cluster_resources",
        return_value=ExecutionResources(cpu=4.0, memory=32 * GiB),
    ):
        op = HashShuffleOperator(
            input_op=op_mock,
            data_context=DataContext.get_current(),
            key_columns=("id",),
        )

    assert op._aggregator_pool is None
    logical_op_mock.infer_metadata.assert_not_called()


def test_hash_shuffle_uses_bounded_sample_to_start_aggregators(
    ray_start_regular,
):
    logical_op_mock = MagicMock(LogicalOperator)
    logical_op_mock.estimated_num_outputs.return_value = 16

    op_mock = MagicMock(PhysicalOperator)
    op_mock._output_dependencies = []
    op_mock._logical_operators = [logical_op_mock]
    op_mock.num_output_splits.return_value = 1
    op_mock.num_outputs_total.return_value = 4

    with patch(
        "ray.data._internal.execution.operators.hash_shuffle"
        "._get_total_cluster_resources",
        return_value=ExecutionResources(cpu=4.0, memory=32 * GiB),
    ):
        op = HashShuffleOperator(
            input_op=op_mock,
            data_context=DataContext.get_current(),
            key_columns=("id",),
        )

    op._MEMORY_ESTIMATION_SAMPLE_NUM_BUNDLES = 2
    op._memory_estimation_sample_size_limit_bytes = 1 * GiB
    first_bundle = _make_ref_bundle(100)
    second_bundle = _make_ref_bundle(200)

    mock_pool = MagicMock()
    with (
        patch.object(
            op, "_create_aggregator_pool", return_value=mock_pool
        ) as create_pool,
        patch.object(op, "_do_add_input_inner") as replay,
    ):
        op._add_input_inner(first_bundle, input_index=0)

        assert op._aggregator_pool is None
        create_pool.assert_not_called()
        replay.assert_not_called()

        op._add_input_inner(second_bundle, input_index=0)

    create_pool.assert_called_once_with(estimated_dataset_bytes=800)
    mock_pool.start.assert_called_once()
    assert replay.call_args_list == [
        call(first_bundle, 0),
        call(second_bundle, 0),
    ]


def test_hash_shuffle_starts_from_partial_sample_when_inputs_done(
    ray_start_regular,
):
    logical_op_mock = MagicMock(LogicalOperator)
    logical_op_mock.estimated_num_outputs.return_value = 16

    op_mock = MagicMock(PhysicalOperator)
    op_mock._output_dependencies = []
    op_mock._logical_operators = [logical_op_mock]
    op_mock.num_output_splits.return_value = 1
    op_mock.num_outputs_total.return_value = None

    with patch(
        "ray.data._internal.execution.operators.hash_shuffle"
        "._get_total_cluster_resources",
        return_value=ExecutionResources(cpu=4.0, memory=32 * GiB),
    ):
        op = HashShuffleOperator(
            input_op=op_mock,
            data_context=DataContext.get_current(),
            key_columns=("id",),
        )

    first_bundle = _make_ref_bundle(100)
    second_bundle = _make_ref_bundle(200)

    mock_pool = MagicMock()
    with (
        patch.object(
            op, "_create_aggregator_pool", return_value=mock_pool
        ) as create_pool,
        patch.object(op, "_do_add_input_inner") as replay,
    ):
        op._add_input_inner(first_bundle, input_index=0)
        op._add_input_inner(second_bundle, input_index=0)

        assert op._aggregator_pool is None

        op.all_inputs_done()

    create_pool.assert_called_once_with(estimated_dataset_bytes=300)
    mock_pool.start.assert_called_once()
    assert replay.call_args_list == [
        call(first_bundle, 0),
        call(second_bundle, 0),
    ]


def test_partial_aggregate_preserves_sort_after_builder_compaction(
    ray_start_regular,
    monkeypatch,
):
    """Regression test for HashAggregate producing duplicate group rows when
    `TableBlockBuilder.build()` reorders rows across an internal compaction.

    For an `AggregateFnV2` whose accumulator can vary in size between groups
    (here, an empty list vs. a non-empty list), the per-block partial-aggregate
    output is built by `_aggregate` row-by-row via the table builder. If
    `_compact_if_needed` triggered mid-loop, the legacy `build()` placed the
    still-uncompacted (newest) rows in front of the compacted (older) tables,
    breaking the "blocks reaching `_combine_aggregated_blocks` are sorted by
    key" precondition that `heapq.merge` relies on. That precondition violation
    surfaced as duplicate group rows whose count varied with the parallelism
    arg (since parallelism changes per-block row count, and therefore whether
    compaction triggers inside the partial-aggregate loop).

    We force compaction on every row via `MAX_UNCOMPACTED_SIZE_BYTES=1` and
    assert that the partial-aggregate output is still sorted by the group key.
    """
    import ray.data._internal.table_block as table_block

    class EmptyAccumulatorForOddKeys(AggregateFnV2):
        def __init__(self):
            super().__init__(
                name="items",
                on=None,
                ignore_nulls=False,
                zero_factory=lambda: [],
            )

        def aggregate_block(self, block):
            table = BlockAccessor.for_block(block).to_arrow()
            group_key = table.column("A")[0].as_py()
            return [] if group_key % 2 else ["value"]

        def combine(self, current, new):
            return current + new

    monkeypatch.setattr(table_block, "MAX_UNCOMPACTED_SIZE_BYTES", 1)

    source = pa.table({"A": [1, 2, 3, 4], "B": [0, 0, 0, 0]})
    partial = BlockAccessor.for_block(source)._aggregate(
        SortKey("A"), (EmptyAccumulatorForOddKeys(),)
    )

    assert partial.column("A").to_pylist() == [1, 2, 3, 4]


def _make_hash_shuffle_op(num_partitions=8):
    logical_op_mock = MagicMock(LogicalOperator)
    logical_op_mock.estimated_num_outputs.return_value = num_partitions

    op_mock = MagicMock(PhysicalOperator)
    op_mock._output_dependencies = []
    op_mock._logical_operators = [logical_op_mock]
    op_mock.num_output_splits.return_value = 1
    op_mock.num_outputs_total.return_value = num_partitions

    with patch(
        "ray.data._internal.execution.operators.hash_shuffle"
        "._get_total_cluster_resources",
        return_value=ExecutionResources(cpu=4.0, memory=32 * GiB),
    ):
        op = HashShuffleOperator(
            input_op=op_mock,
            data_context=DataContext.get_current(),
            key_columns=("id",),
            num_partitions=num_partitions,
        )
    return op


def _set_partition_bytes(op, sizes: Dict[int, int], seq_id: int = 0):
    """Populate observed per-partition stats and mark them pending finalization."""
    for pid, byte_size in sizes.items():
        op._partitions_stats[seq_id][pid] = _PartitionStats(
            num_rows=1, byte_size=byte_size
        )
    op._pending_finalization_partition_ids = set(sizes.keys())


def test_estimate_finalization_memory_req_sums_sequences_with_overhead(
    ray_start_regular,
):
    op = _make_hash_shuffle_op(num_partitions=4)
    # Same partition present in two input sequences (e.g. a join).
    op._partitions_stats[0][2] = _PartitionStats(num_rows=1, byte_size=100)
    op._partitions_stats[1][2] = _PartitionStats(num_rows=1, byte_size=300)

    # (100 + 300) * overhead(2.0)
    assert op._estimate_finalization_memory_req(2) == 800


@dataclass
class FinalizeSelectionCase:
    name: str
    # Pending partition_id -> observed byte size (cost = bytes * overhead(2.0)).
    sizes: Dict[int, int]
    # partition_id -> aggregator id is ``pid % num_aggregators``.
    num_aggregators: int
    # aggregator_id -> hosting node id.
    node_ids: Dict[int, str]
    # node_id -> per-node finalize budget bytes ({} => unknown; count-based only).
    node_budgets: Dict[str, int]
    max_new_tasks: int
    expected_count: int
    # Already-running finalizations: partition_id -> observed byte size.
    in_flight: Dict[int, int] = field(default_factory=dict)
    # Max concurrent finalize tasks per aggregator (default high to isolate the
    # per-node budget behaviour unless a case is exercising the cap).
    per_aggregator_cap: int = 10**9
    # When set, assert the selected partitions' per-node cost stays in budget.
    within_budget: bool = False


# All aggregators co-located on a single node "n0".
_ONE_NODE_8 = dict.fromkeys(range(8), "n0")


@pytest.mark.parametrize(
    "case",
    [
        # Single node; budget fits exactly 3 partitions (3 * 100 * 2.0 = 600).
        FinalizeSelectionCase(
            name="respects_per_node_budget",
            sizes=dict.fromkeys(range(8), 100),
            num_aggregators=8,
            node_ids=_ONE_NODE_8,
            node_budgets={"n0": 600},
            max_new_tasks=8,
            expected_count=3,
            within_budget=True,
        ),
        # Plenty of budget -> bounded by the concurrent-task count cap.
        FinalizeSelectionCase(
            name="caps_at_max_new_tasks",
            sizes=dict.fromkeys(range(8), 1),
            num_aggregators=8,
            node_ids=_ONE_NODE_8,
            node_budgets={"n0": 10**12},
            max_new_tasks=2,
            expected_count=2,
        ),
        # Every partition alone exceeds its node's budget; the node is idle, so
        # exactly one runs alone (per-node forward progress).
        FinalizeSelectionCase(
            name="oversized_runs_alone_on_node",
            sizes=dict.fromkeys(range(4), 1000),
            num_aggregators=4,
            node_ids=dict.fromkeys(range(4), "n0"),
            node_budgets={"n0": 500},
            max_new_tasks=4,
            expected_count=1,
        ),
        # In-flight finalize memory on the node already exceeds its budget -> defer.
        FinalizeSelectionCase(
            name="defers_when_in_flight_consumes_node_budget",
            sizes={0: 100, 1: 100, 2: 100},
            num_aggregators=8,
            node_ids=_ONE_NODE_8,
            node_budgets={"n0": 300},
            max_new_tasks=4,
            expected_count=0,
            in_flight={9: 1000},
        ),
        # Unknown placement / budget -> gate on the task count only.
        FinalizeSelectionCase(
            name="unknown_placement_gates_on_count",
            sizes=dict.fromkeys(range(8), 10**9),
            num_aggregators=8,
            node_ids={},
            node_budgets={},
            max_new_tasks=5,
            expected_count=5,
        ),
        # Two nodes, independent budgets: each admits 2 (2*100*2.0=400), so the
        # cluster runs 4 concurrently -- a single cluster-wide budget of 400 would
        # have allowed only 2.
        FinalizeSelectionCase(
            name="per_node_budgets_are_independent",
            sizes=dict.fromkeys(range(8), 100),
            num_aggregators=4,
            node_ids={0: "a", 1: "a", 2: "b", 3: "b"},
            node_budgets={"a": 400, "b": 400},
            max_new_tasks=8,
            expected_count=4,
            within_budget=True,
        ),
        # Per-aggregator cap binds before the (huge) node budget: 2 aggregators x
        # cap 2 = 4, even though all 8 partitions would otherwise fit.
        FinalizeSelectionCase(
            name="per_aggregator_cap_binds",
            sizes=dict.fromkeys(range(8), 1),
            num_aggregators=2,
            node_ids={0: "n0", 1: "n0"},
            node_budgets={"n0": 10**12},
            max_new_tasks=8,
            expected_count=4,
            per_aggregator_cap=2,
        ),
        # Every partition fires in a single round: each node's budget covers all
        # of its partitions (4 * 100 * 2.0 = 800) and the per-aggregator cap (2)
        # admits both partitions on each aggregator. All 8 run simultaneously.
        FinalizeSelectionCase(
            name="fires_all_when_node_budget_permits",
            sizes=dict.fromkeys(range(8), 100),
            num_aggregators=4,
            node_ids={0: "a", 1: "a", 2: "b", 3: "b"},
            node_budgets={"a": 800, "b": 800},
            max_new_tasks=8,
            expected_count=8,
            per_aggregator_cap=2,
            within_budget=True,
        ),
    ],
    ids=lambda c: c.name,
)
def test_select_partitions_to_finalize(case, ray_start_regular):
    op = _make_hash_shuffle_op()
    _set_partition_bytes(op, case.sizes)

    # Inject placement / budgets directly so the selection logic can be unit
    # tested without a started aggregator pool.
    op._num_aggregators = case.num_aggregators
    op._aggregator_node_ids = case.node_ids
    op._finalize_node_budgets_bytes = case.node_budgets
    op._FINALIZE_MAX_CONCURRENT_TASKS_PER_AGGREGATOR = case.per_aggregator_cap

    # Register any already-running finalizations (with their observed sizes).
    for pid, byte_size in case.in_flight.items():
        op._partitions_stats[0][pid] = _PartitionStats(num_rows=1, byte_size=byte_size)
        op._finalizing_tasks[pid] = MagicMock()

    # Selection order is randomized, so assert the invariants across repeated runs.
    for _ in range(20):
        selected = op._select_partitions_to_finalize(max_new_tasks=case.max_new_tasks)

        assert len(selected) == case.expected_count
        # Only pending (not in-flight) partitions are ever selected.
        assert set(selected) <= op._pending_finalization_partition_ids
        if case.within_budget:
            bytes_by_node: Dict[str, int] = {}
            for pid in selected:
                node_id = case.node_ids[pid % case.num_aggregators]
                bytes_by_node[node_id] = bytes_by_node.get(
                    node_id, 0
                ) + op._estimate_finalization_memory_req(pid)
            for node_id, used in bytes_by_node.items():
                assert used <= case.node_budgets[node_id]


def test_finalize_node_budgets_from_real_placement(ray_start_regular):
    """End-to-end: resolve aggregator placement and derive per-node budgets."""
    op = _make_hash_shuffle_op(num_partitions=4)
    pool = _create_aggregator_pool_for_test(op, 2 * GiB)
    pool.start()
    try:
        node_ids = pool.get_aggregator_node_ids()
        # One entry per aggregator, each on a live node.
        assert set(node_ids.keys()) == set(range(pool.num_aggregators))
        alive_node_ids = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
        assert set(node_ids.values()) <= alive_node_ids

        budgets = op._get_finalize_node_budgets_bytes()
        assert budgets is not None
        # Placement was resolved and cached as a side effect.
        assert op._aggregator_node_ids == node_ids

        node_memory = {
            n["NodeID"]: n["Resources"].get("memory", 0) for n in ray.nodes()
        }
        assert set(budgets.keys()) == set(node_ids.values())
        for node_id, budget in budgets.items():
            assert budget == math.floor(
                node_memory[node_id] * op._FINALIZE_MEMORY_BUDGET_FRACTION
            )
    finally:
        pool.shutdown(force=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
