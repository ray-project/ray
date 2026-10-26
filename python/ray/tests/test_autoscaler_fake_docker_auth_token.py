import os
import subprocess
import sys

import pytest

from ray._common.test_utils import wait_for_condition
from ray.autoscaler._private.fake_multi_node.node_provider import FAKE_HEAD_NODE_ID
from ray.autoscaler._private.fake_multi_node.test_utils import DockerCluster

COUNT_ALIVE_NODES = (
    "import ray; ray.init(); print(sum(n['Alive'] for n in ray.nodes()))"
)


@pytest.fixture
def token_auth_docker_cluster(monkeypatch, tmp_path):
    token_path = tmp_path / "auth_token"
    token_path.write_text("fake-docker-cluster-token")
    token_path.chmod(0o600)
    monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(token_path))
    monkeypatch.delenv("RAY_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("RAY_AUTH_MODE", raising=False)

    cluster = DockerCluster(
        {
            "provider": {
                "image": os.environ.get(
                    "RAY_DOCKER_IMAGE", "rayproject/ray:ha_integration"
                ),
                "env_vars": {"RAY_AUTH_MODE": "token"},
            },
            "available_node_types": {
                "ray.worker.cpu": {"min_workers": 1, "max_workers": 1},
                "ray.worker.gpu": {"min_workers": 0, "max_workers": 0},
            },
        }
    )
    cluster.setup()
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.stop()
        cluster.teardown()


def count_alive_nodes_from_head(cluster: DockerCluster) -> int:
    head_container = cluster._get_docker_container(FAKE_HEAD_NODE_ID)
    if not head_container:
        return 0
    result = subprocess.run(
        ["docker", "exec", head_container, "python", "-c", COUNT_ALIVE_NODES],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        # Shows an auth failure in the test log instead of only a timeout.
        print(result.stderr)
        return 0
    return int(result.stdout.strip().splitlines()[-1])


@pytest.mark.timeout(800)
def test_worker_joins_token_auth_cluster(token_auth_docker_cluster):
    wait_for_condition(
        lambda: count_alive_nodes_from_head(token_auth_docker_cluster) == 2,
        timeout=600,
        retry_interval_ms=5000,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
