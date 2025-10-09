import pytest

import ray
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.deployment_state import DeploymentVersion


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
