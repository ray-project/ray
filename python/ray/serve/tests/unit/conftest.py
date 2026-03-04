from typing import Tuple
from unittest.mock import Mock, patch

import pytest

import ray
from ray._raylet import NodeID
from ray.serve._private.autoscaling_state import AutoscalingStateManager
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve._private.test_utils import (
    MockClusterNodeInfoCache,
    MockKVStore,
    MockReplicaActorWrapper,
    MockTimer,
    dead_replicas_context,
    replica_rank_context,
)


@pytest.fixture(autouse=True)
def disallow_ray_init(monkeypatch):
    def raise_on_init():
        raise RuntimeError("Unit tests should not depend on Ray being initialized.")

    monkeypatch.setattr(ray, "init", raise_on_init)


@pytest.fixture
def mock_deployment_state_manager(
    request,
) -> Tuple[DeploymentStateManager, MockTimer, Mock, Mock]:
    """Fully mocked deployment state manager.

    i.e kv store and gcs client is mocked so we don't need to initialize
    ray. Also, since this is used for some recovery tests, this yields a
    method for creating a new mocked deployment state manager.
    """

    timer = MockTimer()
    with patch(
        "ray.serve._private.deployment_state.ActorReplicaWrapper",
        new=MockReplicaActorWrapper,
    ), patch("time.time", new=timer.time), patch(
        "ray.serve._private.long_poll.LongPollHost"
    ) as mock_long_poll, patch(
        "ray.get_runtime_context"
    ):
        kv_store = MockKVStore()
        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node(NodeID.from_random().hex())
        autoscaling_state_manager = AutoscalingStateManager()

        def create_deployment_state_manager(
            actor_names=None,
            placement_group_names=None,
            create_placement_group_fn_override=None,
        ):
            if actor_names is None:
                actor_names = []

            if placement_group_names is None:
                placement_group_names = []

            return DeploymentStateManager(
                kv_store,
                mock_long_poll,
                actor_names,
                placement_group_names,
                cluster_node_info_cache,
                autoscaling_state_manager,
                head_node_id_override="fake-head-node-id",
                create_placement_group_fn_override=create_placement_group_fn_override,
            )

        yield (
            create_deployment_state_manager,
            timer,
            cluster_node_info_cache,
            autoscaling_state_manager,
        )

        dead_replicas_context.clear()
        replica_rank_context.clear()
