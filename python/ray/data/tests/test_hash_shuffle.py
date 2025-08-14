from unittest.mock import MagicMock

from ray.data import DataContext, ExecutionResources
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.join import JoinOperator
from ray.data._internal.logical.operators.join_operator import JoinType
from ray.data._internal.util import GiB


def test_default_shuffle_aggregator_args(ray_start_regular):
    parent_op_mock = MagicMock(PhysicalOperator)
    parent_op_mock._output_dependencies = []

    op = JoinOperator(
        left_input_op=parent_op_mock,
        right_input_op=parent_op_mock,
        data_context=DataContext.get_current(),
        left_key_columns=("id",),
        right_key_columns=("id",),
        join_type=JoinType.INNER,
        num_partitions=16,
    )

    # - 1 partition per aggregator
    # - No partition size hint
    args = op._get_default_aggregator_ray_remote_args(
        num_partitions=16,
        num_aggregators=16,
        total_available_cluster_resources=ExecutionResources(cpu=4),
        partition_size_hint=None,
    )

    assert {
       "num_cpus": 0.025,    # 4 cores * 10% / 16
       "memory": 939524096,
       "scheduling_strategy": "SPREAD",
    } == args

    # - 4 partitions per aggregator
    # - No partition size hint
    args = op._get_default_aggregator_ray_remote_args(
        num_partitions=64,
        num_aggregators=16,
        total_available_cluster_resources=ExecutionResources(cpu=8),
        partition_size_hint=None,
    )

    assert {
       "num_cpus": 0.05,    # 8 cores * 10% / 16
       "memory": 1744830464,
       "scheduling_strategy": "SPREAD",
    } == args

    # - 4 partitions per aggregator
    # - No partition size hint
    args = op._get_default_aggregator_ray_remote_args(
        num_partitions=64,
        num_aggregators=16,
        total_available_cluster_resources=ExecutionResources(cpu=1),
        partition_size_hint=1 * GiB,
    )

    assert {
       "num_cpus": 0.00625,    # 1 cores * 10% / 16
       "memory": 13958643712,
       "scheduling_strategy": "SPREAD",
    } == args
