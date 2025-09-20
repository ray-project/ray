from dataclasses import dataclass
from typing import Dict, Optional, Any
from unittest.mock import MagicMock, patch

import pytest

from ray.data import DataContext
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.join import JoinOperator
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.join_operator import JoinType
from ray.data._internal.util import GiB
from ray.data.block import BlockMetadata


@dataclass
class JoinTestCase:
    # Expected outputs
    expected_ray_remote_args: Dict[str, Any]
    expected_num_partitions: int
    expected_num_aggregators: int

    # Input dataset configurations
    left_size_bytes: int
    right_size_bytes: int
    left_num_blocks: int
    right_num_blocks: int

    # Join configuration
    target_num_partitions: Optional[int]

    # Cluster resources (for testing different resource scenarios)
    total_cpu: float = 4.0
    total_memory: int = 32 * GiB



@pytest.mark.parametrize(
    "tc", [
        # Branch 1: Auto-derived partitions with limited CPUs
        JoinTestCase(
            left_size_bytes=1 * GiB,
            right_size_bytes=2 * GiB,
            left_num_blocks=10,
            right_num_blocks=5,
            target_num_partitions=None,     # Auto-derive
            total_cpu=4.0,
            expected_num_partitions=10,     # max(10, 5)
            expected_num_aggregators=4,     # min(10 partitions, 4 CPUs) = 4
            expected_ray_remote_args={
                "max_concurrency": 3,       # ceil(10 partitions / 4 aggregators)
                "num_cpus": 0.25,           # 4 CPUs * 25% / 4 aggregators
                "memory": 1771674012,
                "scheduling_strategy": "SPREAD",
            },
        ),

        # Branch 2: Single partition (much higher memory overhead)
        JoinTestCase(
            left_size_bytes=1 * GiB,
            right_size_bytes=1 * GiB,
            left_num_blocks=10,
            right_num_blocks=10,
            target_num_partitions=1,
            total_cpu=4.0,
            expected_num_partitions=1,
            expected_num_aggregators=1,     # min(1 partition, 4 CPUs) = 1
            expected_ray_remote_args={
                "max_concurrency": 1,
                "num_cpus": 1.0,            # 4 CPUs * 25% / 1 aggregator
                "memory": 8589934592,
                "scheduling_strategy": "SPREAD",
            },
        ),

        # Branch 3: Limited CPU resources affecting num_cpus calculation
        JoinTestCase(
            left_size_bytes=2 * GiB,
            right_size_bytes=2 * GiB,
            left_num_blocks=20,
            right_num_blocks=20,
            target_num_partitions=40,
            total_cpu=2.0,                  # Only 2 CPUs available
            expected_num_partitions=40,
            expected_num_aggregators=2,     # min(40 partitions, 2 CPUs) = 2
            expected_ray_remote_args={
                "max_concurrency": 8,       # min(ceil(40/2), 8) = 8
                "num_cpus": 0.25,           # 2 CPUs * 25% / 2 aggregators
                "memory": 2469606197,
                "scheduling_strategy": "SPREAD",
            }
        ),

        # Branch 4: Testing with many CPUs and partitions
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
                "max_concurrency": 4,   # ceil(100 / 32)
                "num_cpus": 0.25,       # 32 CPUs * 25% / 32 aggregators
                "memory": 1315333735,
                "scheduling_strategy": "SPREAD",
            },
        ),

        # Branch 5: Testing max aggregators cap (128 default)
        JoinTestCase(
            left_size_bytes=50 * GiB,
            right_size_bytes=50 * GiB,
            left_num_blocks=200,
            right_num_blocks=200,
            target_num_partitions=200,
            total_cpu=256.0,  # Many CPUs
            expected_num_partitions=200,
            expected_num_aggregators=128,  # min(200, min(256, 128 default max))
            expected_ray_remote_args={
                "max_concurrency": 2,   # ceil(200 / 128)
                "num_cpus": 0.5,        # 256 CPUs * 25% / 128 aggregators
                "memory": 2449473536,
                "scheduling_strategy": "SPREAD",
            },
        ),
    ],
    ids=lambda tc: str(tc)
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

    # Patch the total cluster resources
    with patch(
        'ray.data._internal.execution.operators.hash_shuffle.ray.cluster_resources',
        return_value={"CPU": tc.total_cpu, "memory": tc.total_memory}
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

        assert op._aggregator_pool.num_aggregators == tc.expected_num_aggregators
        assert op._aggregator_pool._aggregator_ray_remote_args == tc.expected_ray_remote_args
