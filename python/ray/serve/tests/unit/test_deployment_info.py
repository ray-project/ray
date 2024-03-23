from typing import Optional
from unittest.mock import patch

import pytest

from ray.serve._private.common import TargetCapacityDirection
from ray.serve._private.config import DeploymentConfig, ReplicaConfig
from ray.serve._private.deployment_info import DeploymentInfo


@patch("ray.serve._private.deployment_info.ray.get_runtime_context")
@pytest.mark.parametrize("target_capacity", [0, 25.809, 73, 100.0, None])
@pytest.mark.parametrize(
    "target_capacity_direction",
    [TargetCapacityDirection.UP, TargetCapacityDirection.DOWN, None],
)
def test_deployment_info_serialization(
    mock_runtime_context,
    target_capacity: Optional[float],
    target_capacity_direction: Optional[TargetCapacityDirection],
):
    """Checks that deploymet infos can be serialized without losing data."""
    # Mock out the runtime_context call, so Ray doesn't start.
    class MockRuntimeContext:
        def get_job_id(self) -> str:
            return ""

    mock_runtime_context.return_value = MockRuntimeContext()

    def compare_replica_configs(config1: ReplicaConfig, config2: ReplicaConfig):
        """Check whether two replica configs contain the same data."""

        assert config1.serialized_deployment_def == config2.serialized_deployment_def
        assert config1.serialized_init_args == config2.serialized_init_args
        assert config1.serialized_init_kwargs == config2.serialized_init_kwargs
        assert config1.ray_actor_options == config2.ray_actor_options
        assert config1.placement_group_bundles == config2.placement_group_bundles
        assert config1.placement_group_strategy == config2.placement_group_strategy
        assert config1.max_replicas_per_node == config2.max_replicas_per_node

    def compare_deployment_info(info1: DeploymentInfo, info2: DeploymentInfo):
        """Checks that two deployment infos contain the same data.

        Does not compare job_ids because those are mocked out to avoid starting
        Ray
        """

        assert info1.version == info2.version
        assert info1.deployment_config == info2.deployment_config
        compare_replica_configs(info1.replica_config, info2.replica_config)
        assert info1.start_time_ms == info2.start_time_ms
        assert info1.target_capacity == info2.target_capacity
        assert info1.target_capacity_direction == info2.target_capacity_direction

    deployment_info_args = dict(
        version="123",
        deployment_config=DeploymentConfig(num_replicas=1),
        replica_config=ReplicaConfig.create(lambda x: x),
        start_time_ms=0,
        deployer_job_id="",
    )

    # Check that serialization works correctly when the DeploymentInfo is
    # initialized with the target_capacity info directly.
    info = DeploymentInfo(
        target_capacity=target_capacity,
        target_capacity_direction=target_capacity_direction,
        **deployment_info_args
    )
    serialized_info = info.to_proto()
    reconstructed_info = DeploymentInfo.from_proto(serialized_info)
    compare_deployment_info(reconstructed_info, info)

    # Check that serialization works correctly when the DeploymentInfo's
    # target_capacity has been set using set_target_capacity().
    info = DeploymentInfo(**deployment_info_args)
    info.set_target_capacity(
        new_target_capacity=target_capacity,
        new_target_capacity_direction=target_capacity_direction,
    )
    serialized_info = info.to_proto()
    reconstructed_info = DeploymentInfo.from_proto(serialized_info)
    compare_deployment_info(reconstructed_info, info)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
