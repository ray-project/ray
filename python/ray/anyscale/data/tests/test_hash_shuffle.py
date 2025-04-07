import ray
import pytest

from unittest.mock import MagicMock, patch
from typing import Dict

from ray.anyscale.data._internal.execution.operators.hash_shuffle import (
    HashShuffleOperator,
    AggregatorPool,
)
from ray.core.generated import autoscaler_pb2
from ray.data import DataContext
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.util import GiB


def test_derive_final_shuffle_aggregator_remote_args():

    # Case 1: max_concurrency derived from the partition-map
    remote_args = AggregatorPool._derive_final_shuffle_aggregator_ray_remote_args(
        aggregator_ray_remote_args={
            "num_cpus": 1.0,
            "num_gpus": 1.0,
            "memory": 1 * GiB,
        },
        aggregator_partition_map={
            1: [1, 2, 3],
            2: [4, 5, 6, 7],
        },
    )

    assert {
        "num_cpus": 1.0,
        "num_gpus": 1.0,
        "memory": 1 * GiB,
        "max_concurrency": 4,
    } == remote_args

    # Case 2: max_concurrency overridden by the user
    remote_args = AggregatorPool._derive_final_shuffle_aggregator_ray_remote_args(
        aggregator_ray_remote_args={
            "num_cpus": 1.0,
            "num_gpus": 1.0,
            "memory": 1 * GiB,
            "max_concurrency": 1,
        },
        aggregator_partition_map={
            1: [1, 2, 3],
            2: [4, 5, 6, 7],
        },
    )

    assert {
        "num_cpus": 1.0,
        "num_gpus": 1.0,
        "memory": 1 * GiB,
        "max_concurrency": 1,
    } == remote_args


def test_default_shuffle_aggregator_args():
    parent_op_mock = MagicMock(PhysicalOperator)
    parent_op_mock._output_dependencies = []

    with patch.object(
        ray._private.state.state,
        "get_cluster_config",
        return_value=autoscaler_pb2.ClusterConfig(),
    ):
        op = HashShuffleOperator(
            input_op=parent_op_mock,
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

        assert {
            "num_cpus": op._get_default_num_cpus_per_partition() * (16 / 16),
            "memory": 268435456,
            "scheduling_strategy": "SPREAD",
        } == args

        # - 4 partitions per aggregator
        # - No partition size hint
        args = op._get_default_aggregator_ray_remote_args(
            num_partitions=64,
            num_aggregators=16,
            partition_size_hint=None,
        )

        assert {
            "num_cpus": op._get_default_num_cpus_per_partition() * (64 / 16),
            "memory": 671088640,
            "scheduling_strategy": "SPREAD",
        } == args

        # - 4 partitions per aggregator
        args = op._get_default_aggregator_ray_remote_args(
            num_partitions=64,
            num_aggregators=16,
            partition_size_hint=1 * GiB,
        )

        assert {
            "num_cpus": op._get_default_num_cpus_per_partition() * (64 / 16),
            "memory": 5368709120,
            "scheduling_strategy": "SPREAD",
        } == args


@pytest.mark.parametrize(
    "description, max_resources, override_value, num_partitions, num_aggregators, expected_num_cpus",
    [
        (
            "num_cpus_per_partition should be estimated from cluster config",
            {"CPU": 32},
            None,
            16,
            16,
            1,  # min(1, (32 / 2) / 16)
        ),
        (
            "num_cpus_per_partition should be capped at 1",
            {"CPU": 1024},
            None,
            16,
            16,
            1,  # min(1, (1024 / 2) / 16)
        ),
        (
            "num_cpus_per_partition should be estimated from the defaults",
            None,
            None,
            16,
            16,
            0.0625,  # default
        ),
        (
            "num_cpus_per_partition should be estimated from the defaults",
            None,
            None,
            32,
            16,
            0.0625,  # default
        ),
        (
            "num_cpus_per_partition should be equal to override value",
            None,
            0.5,
            16,
            16,
            0.5,  # override
        ),
    ],
)
def test_populating_num_cpus_per_partition_from_cluster_config(
    description: str,
    max_resources: Dict[str, int],
    override_value: int,
    num_partitions: int,
    num_aggregators: int,
    expected_num_cpus: int,
):
    parent_op_mock = MagicMock(PhysicalOperator)
    parent_op_mock._output_dependencies = []

    with patch.object(
        ray._private.state.state,
        "get_max_resources_from_cluster_config",
        return_value=max_resources,
    ):

        data_context = DataContext.get_current()
        if override_value:
            data_context.hash_shuffle_operator_actor_num_cpus_per_partition_override = (
                override_value
            )
        op = HashShuffleOperator(
            input_op=parent_op_mock,
            data_context=DataContext.get_current(),
            key_columns=[("id",)],
            num_partitions=num_partitions,
        )
        assert (
            op._get_aggregator_num_cpus_per_partition(num_partitions=num_partitions)
            == expected_num_cpus
        ), description
