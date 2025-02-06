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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
