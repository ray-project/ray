from unittest.mock import MagicMock

from ray.anyscale.data._internal.execution.operators.hash_shuffle import (
    HashShuffleOperator,
)
from ray.data import DataContext
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.util import GiB


def test_default_shuffle_aggregator_args():
    parent_op_mock = MagicMock(PhysicalOperator)
    parent_op_mock._output_dependencies = []

    op = HashShuffleOperator(
        input_ops=[parent_op_mock],
        data_context=DataContext.get_current(),
        key_columns=[("id",)],
        num_partitions=16,
    )

    # - 1 partition per aggregator
    # - No partition size hint
    args = op._get_default_aggregator_ray_remote_args(
        num_partitions=16,
        num_aggregators=16,
        partition_size_hint=None,
    )

    assert {"num_cpus": 0.125, "memory": 268435456} == args

    # - 4 partitions per aggregator
    # - No partition size hint
    args = op._get_default_aggregator_ray_remote_args(
        num_partitions=64,
        num_aggregators=16,
        partition_size_hint=None,
    )

    assert {"num_cpus": 0.5, "memory": 671088640} == args

    # - 4 partitions per aggregator
    # - No partition size hint
    args = op._get_default_aggregator_ray_remote_args(
        num_partitions=64,
        num_aggregators=16,
        partition_size_hint=1 * GiB,
    )

    assert {"num_cpus": 0.5, "memory": 5368709120} == args
