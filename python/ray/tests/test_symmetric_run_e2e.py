"""End-to-end integration tests for `ray symmetric-run`.

These tests spin up Docker containers with distinct static IPs to verify the
full lifecycle: head detection -> cluster formation -> entrypoint execution -> cleanup.

Requires Docker and the `rayproject/ray:ha_integration` image.
"""

import sys
import textwrap

import pytest

import docker

NETWORK_SUBNET = "192.168.53.0/24"
NETWORK_GATEWAY = "192.168.53.254"
NETWORK_NAME = "symmetric_run_e2e_net"
IMAGE = "rayproject/ray:ha_integration"

HEAD_IP = "192.168.53.10"
WORKER_IPS = ["192.168.53.11", "192.168.53.12"]
ALL_IPS = [HEAD_IP] + WORKER_IPS
GCS_ADDRESS = f"{HEAD_IP}:6379"

# 4x the default 30s timeout, to account for Docker overhead.
CLUSTER_WAIT_TIMEOUT = "120"

# Maximum time (seconds) to wait for the head container to finish.
CONTAINER_TIMEOUT = 180


@pytest.fixture(scope="module")
def docker_client():
    client = docker.from_env()
    yield client
    client.close()


@pytest.fixture(scope="module")
def docker_network(docker_client):
    ipam_pool = docker.types.IPAMPool(subnet=NETWORK_SUBNET, gateway=NETWORK_GATEWAY)
    ipam_config = docker.types.IPAMConfig(pool_configs=[ipam_pool])
    network = docker_client.networks.create(
        NETWORK_NAME, driver="bridge", ipam=ipam_config
    )
    yield network
    network.remove()


def _run_symmetric_run_containers(docker_client, docker_network, entrypoint_script):
    """Start 3 containers all running the same symmetric-run command.

    Returns (head_container, [worker_containers]).
    """
    containers = []

    for ip in ALL_IPS:
        container = docker_client.containers.run(
            IMAGE,
            command=[
                "ray",
                "symmetric-run",
                "--address",
                GCS_ADDRESS,
                "--min-nodes",
                "3",
                "--",
                "python",
                "-c",
                entrypoint_script,
            ],
            environment={
                "RAY_SYMMETRIC_RUN_CLUSTER_WAIT_TIMEOUT": CLUSTER_WAIT_TIMEOUT,
            },
            network=docker_network.name,
            detach=True,
        )
        # Assign static IP by connecting to the network with the desired IP
        # (disconnect from default attachment first, then reconnect with IP).
        docker_network.disconnect(container)
        docker_network.connect(container, ipv4_address=ip)
        containers.append(container)

    head = containers[0]
    workers = containers[1:]
    return head, workers


def _cleanup_containers(containers):
    for c in containers:
        try:
            c.kill()
        except docker.errors.APIError:
            pass  # already exited
        try:
            c.remove(force=True)
        except docker.errors.APIError:
            pass


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_symmetric_run_e2e(docker_client, docker_network):
    """Verify that symmetric-run forms a 3-node cluster and runs the entrypoint."""
    entrypoint = textwrap.dedent(
        """\
        import ray
        import time

        ray.init(ignore_reinit_error=True)

        # Verify all 3 nodes joined
        alive = [n for n in ray.nodes() if n["Alive"]]
        assert len(alive) == 3, f"Expected 3 alive nodes, got {len(alive)}"

        # Verify cross-node task execution works
        @ray.remote
        def get_node_id():
            return ray.get_runtime_context().get_node_id()

        node_ids = set(ray.get([get_node_id.remote() for _ in range(30)]))
        assert len(node_ids) >= 2, f"Expected tasks on >=2 nodes, got {len(node_ids)}"

        print("SYMMETRIC_RUN_E2E_SUCCESS")
    """
    )

    head, workers = _run_symmetric_run_containers(
        docker_client, docker_network, entrypoint
    )
    all_containers = [head] + workers

    try:
        result = head.wait(timeout=CONTAINER_TIMEOUT)
        exit_code = result["StatusCode"]

        # Print logs for debugging on failure
        if exit_code != 0:
            print("=== HEAD LOGS ===")
            print(head.logs().decode())
            for i, w in enumerate(workers):
                print(f"=== WORKER {i} LOGS ===")
                print(w.logs().decode())

        assert exit_code == 0, (
            f"Head container exited with code {exit_code}. "
            f"Logs:\n{head.logs().decode()}"
        )

        head_logs = head.logs().decode()
        assert "SYMMETRIC_RUN_E2E_SUCCESS" in head_logs
    finally:
        _cleanup_containers(all_containers)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_symmetric_run_exit_code_propagation(docker_client, docker_network):
    """Verify that the entrypoint exit code is propagated through symmetric-run."""
    entrypoint = textwrap.dedent(
        """\
        import sys
        sys.exit(42)
    """
    )

    head, workers = _run_symmetric_run_containers(
        docker_client, docker_network, entrypoint
    )
    all_containers = [head] + workers

    try:
        result = head.wait(timeout=CONTAINER_TIMEOUT)
        exit_code = result["StatusCode"]

        assert exit_code == 42, (
            f"Expected exit code 42, got {exit_code}. " f"Logs:\n{head.logs().decode()}"
        )
    finally:
        _cleanup_containers(all_containers)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
