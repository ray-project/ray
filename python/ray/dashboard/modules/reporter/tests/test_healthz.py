import signal
import sys

import pytest
import requests

import ray._private.ray_constants as ray_constants
from ray._common.network_utils import find_free_port
from ray._common.test_utils import wait_for_condition
from ray.tests.conftest import *  # noqa: F401 F403


def test_healthz_head(monkeypatch, ray_start_cluster):
    dashboard_port = find_free_port()
    h = ray_start_cluster.add_node(dashboard_port=dashboard_port)
    uri = f"http://localhost:{dashboard_port}/api/gcs_healthz"
    wait_for_condition(lambda: requests.get(uri).status_code == 200)
    h.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0].process.kill()
    # It'll either timeout or just return an error
    try:
        wait_for_condition(lambda: requests.get(uri, timeout=1) != 200, timeout=4)
    except RuntimeError as e:
        assert "Read timed out" in str(e)


def test_healthz_agent_1(monkeypatch, ray_start_cluster):
    agent_port = find_free_port()
    h = ray_start_cluster.add_node(dashboard_agent_listen_port=agent_port)
    uri = f"http://{h.node_ip_address}:{agent_port}/api/local_raylet_healthz"

    wait_for_condition(lambda: requests.get(uri).status_code == 200)

    h.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0].process.kill()
    # GCS's failure will not lead to healthz failure
    assert requests.get(uri).status_code == 200


@pytest.mark.skipif(sys.platform == "win32", reason="SIGSTOP only on posix")
def test_healthz_agent_2(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_health_check_failure_threshold", "3")
    monkeypatch.setenv("RAY_health_check_timeout_ms", "100")
    monkeypatch.setenv("RAY_health_check_period_ms", "1000")
    monkeypatch.setenv("RAY_health_check_initial_delay_ms", "0")

    agent_port = find_free_port()
    h = ray_start_cluster.add_node(dashboard_agent_listen_port=agent_port)
    uri = f"http://{h.node_ip_address}:{agent_port}/api/local_raylet_healthz"

    wait_for_condition(lambda: requests.get(uri).status_code == 200)

    h.all_processes[ray_constants.PROCESS_TYPE_RAYLET][0].process.send_signal(
        signal.SIGSTOP
    )

    # GCS still think raylet is alive.
    assert requests.get(uri).status_code == 200
    # But after heartbeat timeout, it'll think the raylet is down.
    wait_for_condition(lambda: requests.get(uri).status_code != 200)


def test_unified_healthz_head(monkeypatch, ray_start_cluster):
    agent_port = find_free_port()
    h = ray_start_cluster.add_node(dashboard_agent_listen_port=agent_port)
    uri = f"http://{h.node_ip_address}:{agent_port}/api/healthz"

    wait_for_condition(lambda: requests.get(uri).status_code == 200)
    resp = requests.get(uri)
    assert "raylet: success" in resp.text
    assert "gcs: success" in resp.text

    h.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0].process.kill()
    wait_for_condition(lambda: requests.get(uri).status_code == 503)
    resp = requests.get(uri)
    assert "gcs: " in resp.text
    assert "gcs: success" not in resp.text


@pytest.mark.skipif(sys.platform == "win32", reason="SIGSTOP only on posix")
def test_unified_healthz_worker(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_health_check_failure_threshold", "3")
    monkeypatch.setenv("RAY_health_check_timeout_ms", "100")
    monkeypatch.setenv("RAY_health_check_period_ms", "1000")
    monkeypatch.setenv("RAY_health_check_initial_delay_ms", "0")

    ray_start_cluster.add_node()
    agent_port = find_free_port()
    h = ray_start_cluster.add_node(dashboard_agent_listen_port=agent_port)
    uri = f"http://{h.node_ip_address}:{agent_port}/api/healthz"

    wait_for_condition(lambda: requests.get(uri).status_code == 200)
    resp = requests.get(uri)
    assert "gcs: success (no local gcs)" in resp.text

    # Stop local raylet and verify this makes /healthz fail.
    h.all_processes[ray_constants.PROCESS_TYPE_RAYLET][0].process.send_signal(
        signal.SIGSTOP
    )
    wait_for_condition(lambda: requests.get(uri).status_code == 503)
    resp = requests.get(uri)
    assert "raylet: Local Raylet failed" in resp.text


def test_unified_healthz_worker_gcs_down(monkeypatch, ray_start_cluster):
    h_head = ray_start_cluster.add_node()
    agent_port = find_free_port()
    h_worker = ray_start_cluster.add_node(dashboard_agent_listen_port=agent_port)
    uri = f"http://{h_worker.node_ip_address}:{agent_port}/api/healthz"

    wait_for_condition(lambda: requests.get(uri).status_code == 200)
    resp = requests.get(uri)
    assert "gcs: success (no local gcs)" in resp.text

    # Stop the head GCS server.
    h_head.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0].process.kill()

    # Worker health check should still succeed.
    assert requests.get(uri).status_code == 200


def test_gcs_readiness_normal(ray_start_cluster):
    dashboard_port = find_free_port()
    ray_start_cluster.add_node(dashboard_port=dashboard_port)
    uri = f"http://localhost:{dashboard_port}/api/gcs_readiness"
    wait_for_condition(lambda: requests.get(uri).status_code == 200)


def test_healthz_passive(monkeypatch, external_redis, ray_start_cluster):
    from ray._private.node import Node
    from ray._private.parameter import RayParams

    # 1. Start leader node to populate ClusterID and run as active GCS
    h_leader = ray_start_cluster.add_node()

    # 2. Start passive node with head=True and RAY_LEADER_ELECT env var
    dashboard_port = find_free_port()
    agent_port = find_free_port()

    passive_params = RayParams(
        redis_address=h_leader.redis_address,
        redis_username=ray_constants.REDIS_DEFAULT_USERNAME,
        redis_password=ray_constants.REDIS_DEFAULT_PASSWORD,
        dashboard_port=dashboard_port,
        dashboard_agent_listen_port=agent_port,
        num_cpus=1,
        num_gpus=0,
        object_store_memory=150 * 1024 * 1024,
        _system_config={"LEADER_ELECT": True},
        env_vars={"RAY_LEADER_ELECT": "true"},
    )

    h_passive = Node(
        passive_params,
        head=True,
        shutdown_at_exit=ray_start_cluster._shutdown_at_exit,
        spawn_reaper=ray_start_cluster._shutdown_at_exit,
    )
    ray_start_cluster.worker_nodes.add(h_passive)

    # 3. Test GCS specific endpoints on passive node
    gcs_healthz_uri = f"http://localhost:{dashboard_port}/api/gcs_healthz"
    gcs_readiness_uri = f"http://localhost:{dashboard_port}/api/gcs_readiness"

    # GCS liveness check (/api/gcs_healthz) should still succeed because it doesn't verify leadership
    wait_for_condition(lambda: requests.get(gcs_healthz_uri).status_code == 200)

    # GCS readiness check (/api/gcs_readiness) should fail with 503 because GCS is passive
    resp = requests.get(gcs_readiness_uri)
    assert resp.status_code == 503

    # 4. Test unified /api/healthz endpoint on passive node
    unified_uri = f"http://{h_passive.node_ip_address}:{agent_port}/api/healthz"

    # Unified liveness check (/api/healthz) should succeed
    wait_for_condition(lambda: requests.get(unified_uri).status_code == 200)

    # Unified readiness check (/api/healthz?readiness=true) should fail with 503
    unified_readiness_uri = f"{unified_uri}?readiness=true"
    resp = requests.get(unified_readiness_uri)
    assert resp.status_code == 503
    assert "gcs: GCS readiness check failed." in resp.text


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
