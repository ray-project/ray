import sys
import types
from typing import Optional
from unittest.mock import patch

import pytest

from ray import cloudpickle
from ray.serve._private import deployment_info as deployment_info_module
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


@pytest.fixture
def stub_actor_class_build(monkeypatch):
    """Isolate the dynamic actor cache and stub `ReplicaActor` + `ray.remote`.

    Yields the list of `ray.remote(...)` invocations so tests can assert how
    many times the dynamic class was actually (re)built.
    """
    monkeypatch.setattr(deployment_info_module, "_DYNAMIC_ACTOR_CLASS_CACHE", {})

    class _FakeReplicaActor:
        pass

    fake_replica_module = types.ModuleType("ray.serve._private.replica")
    fake_replica_module.ReplicaActor = _FakeReplicaActor
    monkeypatch.setitem(sys.modules, "ray.serve._private.replica", fake_replica_module)

    remote_calls = []

    def _fake_remote(cls):
        remote_calls.append(cls)
        return cls

    monkeypatch.setattr(deployment_info_module.ray, "remote", _fake_remote)

    yield remote_calls


def _make_info(actor_name: str) -> DeploymentInfo:
    return DeploymentInfo(
        deployment_config=DeploymentConfig(num_replicas=1),
        replica_config=ReplicaConfig.create(lambda x: x),
        start_time_ms=0,
        deployer_job_id="",
        actor_name=actor_name,
    )


def _via_cloudpickle(info: DeploymentInfo) -> DeploymentInfo:
    # Matches the controller's recovery path
    return cloudpickle.loads(cloudpickle.dumps(info))


def _via_update(info: DeploymentInfo) -> DeploymentInfo:
    return info.update(deployment_config=DeploymentConfig(num_replicas=2))


def test_actor_def_caches_dynamic_class_per_actor_name(stub_actor_class_build):
    remote_calls = stub_actor_class_build

    info_a = _make_info("dep_foo")
    info_b = _make_info("dep_foo")
    info_c = _make_info("dep_bar")

    assert info_a.actor_def is info_b.actor_def
    assert info_c.actor_def is not info_a.actor_def
    assert len(remote_calls) == 2


@pytest.mark.parametrize(
    "make_fresh_info",
    [_via_cloudpickle, _via_update],
    ids=["cloudpickle", "update"],
)
def test_actor_def_cache_survives_fresh_instance(
    stub_actor_class_build, make_fresh_info
):
    """A DeploymentInfo with no per-instance cache reuses the global cache."""
    remote_calls = stub_actor_class_build

    info = _make_info("dep_foo")
    original_cls = info.actor_def

    fresh = make_fresh_info(info)
    assert fresh._cached_actor_def is None
    assert fresh.actor_def is original_cls
    assert len(remote_calls) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
