from typing import Optional
from unittest.mock import MagicMock

import pytest

from ray.data import DataContext, ExecutionResources
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.join import JoinOperator
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.join_operator import JoinType
from ray.data._internal.util import GiB
from ray.data.block import BlockMetadata


@pytest.mark.parametrize(
    "target_num_partitions,expected_num_aggregators,expected_ray_remote_args", [
        # User-configured 16 partitions
        (16, 1, {
            "max_concurrency": 8,   # 1 aggregator, 16 partitions (capped at 8)
            "num_cpus": 0.25,       # 1 CPU * 25%
            "memory": 4966055965,   # ~4.7Gb
            "scheduling_strategy": "SPREAD",
        }),
        # User-configured 1 partition (substantially more joining overhead)
        (1, 1, {
            "max_concurrency": 1,   # 1 aggregator
            "num_cpus": 0.25,       # 1 CPU * 25%
            "memory": 15032385620,  # ~15Gb
            "scheduling_strategy": "SPREAD",
        }),
        # Derived 10 partitions
        (None, 1, {
            "max_concurrency": 8,   # 1 aggregator, 10 partitions (capped at 8
            "num_cpus": 0.25,       # 1 CPU * 25%
            "memory": 5368709150,   # ~5Gb
            "scheduling_strategy": "SPREAD",
        }),
    ]
)
def test_default_shuffle_aggregator_args(
    ray_start_regular,
    # input_num_blocks: int,
    expected_num_aggregators,
    target_num_partitions: Optional[int],
    expected_ray_remote_args,
):
    parent_logical_op_mock = MagicMock(LogicalOperator)
    parent_logical_op_mock.infer_metadata.return_value = BlockMetadata(
        num_rows=None,
        size_bytes=1 * GiB,
        exec_stats=None,
        input_files=None,
    )
    parent_logical_op_mock.estimated_num_outputs.return_value = 10

    parent_op_mock = MagicMock(PhysicalOperator)

    parent_op_mock._output_dependencies = []
    parent_op_mock._logical_operators = [parent_logical_op_mock]

    op = JoinOperator(
        left_input_op=parent_op_mock,
        right_input_op=parent_op_mock,
        data_context=DataContext.get_current(),
        left_key_columns=("id",),
        right_key_columns=("id",),
        join_type=JoinType.INNER,
        num_partitions=target_num_partitions,
    )

    assert op._aggregator_pool._aggregator_ray_remote_args == expected_ray_remote_args
    assert op._aggregator_pool.num_aggregators == expected_num_aggregators