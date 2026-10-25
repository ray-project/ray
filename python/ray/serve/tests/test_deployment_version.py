import pytest

import ray
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.deployment_state import DeploymentVersion
from ray.serve.config import GangSchedulingConfig


def test_hash_consistent_across_processes(serve_instance):
    @ray.remote
    def get_version():
        return DeploymentVersion(
            "1",
            DeploymentConfig(user_config=([{"1": "2"}, {"1": "2"}],)),
            {},
        )

    assert len(set(ray.get([get_version.remote() for _ in range(100)]))) == 1


def test_route_prefix_changes_trigger_reconfigure_hash():
    """Test that route prefix changes trigger a reconfigure hash change."""
    cfg = DeploymentConfig()
    v1 = DeploymentVersion(
        code_version="same version",
        deployment_config=cfg,
        ray_actor_options={},
        route_prefix="/a",
    )
    v2 = DeploymentVersion(
        code_version="same version",
        deployment_config=cfg,
        ray_actor_options={},
        route_prefix="/b",
    )
    assert v1.reconfigure_actor_hash != v2.reconfigure_actor_hash
    # Should not require a full actor restart if nothing else changed
    assert not v1.requires_actor_restart(v2)
    assert v1.requires_actor_reconfigure(v2)


def test_placement_group_options_trigger_restart():
    """Test that changing a PG label selector or fallback strategy triggers an actor restart."""
    cfg = DeploymentConfig()

    # Initial deployment.
    v1 = DeploymentVersion(
        code_version="1",
        deployment_config=cfg,
        ray_actor_options={},
        placement_group_bundles=[{"CPU": 1}],
        placement_group_strategy="PACK",
    )

    # Change bundle_label_selector.
    v2 = DeploymentVersion(
        code_version="1",
        deployment_config=cfg,
        ray_actor_options={},
        placement_group_bundles=[{"CPU": 1}],
        placement_group_strategy="PACK",
        placement_group_bundle_label_selector=[{"region": "us-west"}],
    )

    # Validate actor restart occurs due to differing hash.
    assert v1.placement_group_options_hash != v2.placement_group_options_hash
    assert v1.requires_actor_restart(v2)

    # Change fallback_strategy.
    v3 = DeploymentVersion(
        code_version="1",
        deployment_config=cfg,
        ray_actor_options={},
        placement_group_bundles=[{"CPU": 1}],
        placement_group_strategy="PACK",
        placement_group_fallback_strategy=[{"bundles": [{"CPU": 1}]}],
    )

    # Validate actor restart occurs due to differing hash.
    assert v1.placement_group_options_hash != v3.placement_group_options_hash
    assert v1.requires_actor_restart(v3)


@pytest.mark.parametrize(
    "gsc_kwargs,should_restart",
    [
        ({"gang_size": 2}, False),
        ({"gang_size": 4}, True),
        ({"gang_size": 2, "gang_placement_strategy": "PACK"}, False),
        ({"gang_size": 2, "gang_placement_strategy": "SPREAD"}, True),
        (None, True),
    ],
)
def test_gang_scheduling_config_changes_trigger_restart(gsc_kwargs, should_restart):
    """Test that changing gang_scheduling_config triggers an actor restart."""
    config1 = GangSchedulingConfig(gang_size=2)
    config2 = GangSchedulingConfig(**gsc_kwargs) if gsc_kwargs is not None else None

    cfg1 = DeploymentConfig(num_replicas=4, gang_scheduling_config=config1)
    v1 = DeploymentVersion(
        code_version="1", deployment_config=cfg1, ray_actor_options={}
    )

    cfg2 = DeploymentConfig(num_replicas=4, gang_scheduling_config=config2)
    v2 = DeploymentVersion(
        code_version="1", deployment_config=cfg2, ray_actor_options={}
    )

    if should_restart:
        assert v1.gang_scheduling_config_hash != v2.gang_scheduling_config_hash
        assert v1.requires_actor_restart(v2)
    else:
        assert v1.gang_scheduling_config_hash == v2.gang_scheduling_config_hash
        assert not v1.requires_actor_restart(v2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
