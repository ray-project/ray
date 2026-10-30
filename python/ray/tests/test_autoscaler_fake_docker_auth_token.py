import os
import subprocess
import sys
from typing import Iterator, Optional, Tuple

import pytest

from ray._common.test_utils import wait_for_condition
from ray._raylet import AuthenticationMode
from ray.autoscaler._private.fake_multi_node.node_provider import FAKE_HEAD_NODE_ID
from ray.autoscaler._private.fake_multi_node.test_utils import DockerCluster

# Prints the driver's auth mode and the alive node count. A driver whose mode
# differs from the cluster's is rejected, so a count proves the cluster's mode.
CLUSTER_STATE = (
    "import ray; from ray._raylet import get_authentication_mode as g; "
    "ray.init(address='auto'); "
    "print(int(g()), sum(n['Alive'] for n in ray.nodes()))"
)


def start_docker_cluster(yaml_auth_mode: Optional[str]) -> DockerCluster:
    """Starts a fake Docker cluster with one worker, with the cluster YAML's
    auth_mode set to yaml_auth_mode, or left out for None."""
    config = {
        "provider": {
            "image": os.environ.get(
                "RAY_DOCKER_IMAGE", "rayproject/ray:ha_integration"
            ),
        },
        "available_node_types": {
            "ray.worker.cpu": {"min_workers": 1, "max_workers": 1},
            "ray.worker.gpu": {"min_workers": 0, "max_workers": 0},
        },
    }
    if yaml_auth_mode:
        config["auth_mode"] = yaml_auth_mode
    cluster = DockerCluster(config)
    cluster.setup()
    return cluster


@pytest.fixture
def docker_cluster(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> Iterator[DockerCluster]:
    yaml_auth_mode, shell_auth_mode = request.param
    token_path = tmp_path / "auth_token"
    token_path.write_text("fake-docker-cluster-token")
    token_path.chmod(0o600)
    monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(token_path))
    monkeypatch.delenv("RAY_AUTH_TOKEN", raising=False)
    monkeypatch.setenv("RAY_AUTH_MODE", shell_auth_mode)

    cluster = start_docker_cluster(yaml_auth_mode)
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.stop()
        cluster.teardown()


def cluster_state_from_head(cluster: DockerCluster) -> Optional[Tuple[int, int]]:
    """Returns the head driver's auth mode and the alive node count, or None
    while the cluster isn't reachable."""
    head_container = cluster._get_docker_container(FAKE_HEAD_NODE_ID)
    if not head_container:
        return None
    result = subprocess.run(
        ["docker", "exec", head_container, "python", "-c", CLUSTER_STATE],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        # Shows an auth failure in the test log instead of only a timeout.
        print(result.stderr)
        return None
    auth_mode, alive_nodes = result.stdout.strip().splitlines()[-1].split()
    return int(auth_mode), int(alive_nodes)


# The cluster YAML's auth_mode decides, not the RAY_AUTH_MODE of the shell
# running `ray up`. None leaves the key out, which means token mode.
@pytest.mark.parametrize(
    "docker_cluster, expected_auth_mode",
    [
        (("disabled", "token"), AuthenticationMode.DISABLED),
        ((None, "disabled"), AuthenticationMode.TOKEN),
    ],
    indirect=["docker_cluster"],
)
@pytest.mark.timeout(800)
def test_worker_joins_docker_cluster(
    docker_cluster: DockerCluster, expected_auth_mode: int
):
    wait_for_condition(
        lambda: cluster_state_from_head(docker_cluster) == (expected_auth_mode, 2),
        timeout=600,
        retry_interval_ms=5000,
    )
    token_mounted = os.path.exists(
        os.path.join(docker_cluster.cluster_dir, "auth_token")
    )
    assert token_mounted == (expected_auth_mode == AuthenticationMode.TOKEN)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
