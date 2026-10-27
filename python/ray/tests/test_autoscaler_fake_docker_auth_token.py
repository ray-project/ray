import os
import subprocess
import sys
from typing import Iterator, Optional

import pytest

from ray._common.test_utils import wait_for_condition
from ray.autoscaler._private.fake_multi_node.node_provider import FAKE_HEAD_NODE_ID
from ray.autoscaler._private.fake_multi_node.test_utils import DockerCluster

COUNT_ALIVE_NODES = (
    "import ray; ray.init(address='auto'); print(sum(n['Alive'] for n in ray.nodes()))"
)


def start_docker_cluster(auth_mode: Optional[str]) -> DockerCluster:
    """Starts a fake Docker cluster with one worker, with RAY_AUTH_MODE set to
    auth_mode both for `ray up` and on the nodes, or unset for None."""
    node_env = {"RAY_AUTH_MODE": auth_mode} if auth_mode else {}
    cluster = DockerCluster(
        {
            "provider": {
                "image": os.environ.get(
                    "RAY_DOCKER_IMAGE", "rayproject/ray:ha_integration"
                ),
                "env_vars": node_env,
            },
            "available_node_types": {
                "ray.worker.cpu": {"min_workers": 1, "max_workers": 1},
                "ray.worker.gpu": {"min_workers": 0, "max_workers": 0},
            },
        }
    )
    cluster.setup()
    return cluster


@pytest.fixture
def docker_cluster(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> Iterator[DockerCluster]:
    auth_mode = request.param
    token_path = tmp_path / "auth_token"
    token_path.write_text("fake-docker-cluster-token")
    token_path.chmod(0o600)
    monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(token_path))
    monkeypatch.delenv("RAY_AUTH_TOKEN", raising=False)
    if auth_mode:
        monkeypatch.setenv("RAY_AUTH_MODE", auth_mode)
    else:
        monkeypatch.delenv("RAY_AUTH_MODE", raising=False)

    cluster = start_docker_cluster(auth_mode)
    cluster.auth_mode = auth_mode
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


# None leaves RAY_AUTH_MODE unset, which means auth off until AUTH_MODE
# defaults to token: `ray up` must then ship no token.
@pytest.mark.parametrize("docker_cluster", ["token", None], indirect=True)
@pytest.mark.timeout(800)
def test_worker_joins_docker_cluster(docker_cluster: DockerCluster):
    wait_for_condition(
        lambda: count_alive_nodes_from_head(docker_cluster) == 2,
        timeout=600,
        retry_interval_ms=5000,
    )
    token_mounted = os.path.exists(
        os.path.join(docker_cluster.cluster_dir, "auth_token")
    )
    assert token_mounted == (docker_cluster.auth_mode == "token")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
