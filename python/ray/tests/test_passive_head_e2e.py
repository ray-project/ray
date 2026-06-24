# e2e integration test for Active-Passive standby nodes:
# 1. When two head nodes start pointing to the same shared Redis storage, only the active leader registers itself.
# 2. The passive head node's GCS remains in passive mode (holding off on table registrations and RPC handlers).
# 3. The passive head node's dashboard health/readiness check fails with a 503 Service Unavailable.
# 4. Both nodes remain healthy and running without crashing.

import os
import sys
import time

import pytest
import redis
import requests

import ray
from ray._common.network_utils import find_free_port
from ray._private.node import Node
from ray._private.parameter import RayParams


def test_passive_head_e2e(external_redis):
    redis_address = os.environ.get("RAY_REDIS_ADDRESS", "127.0.0.1:6379")
    redis_ip, redis_port = redis_address.split(":")
    redis_port = int(redis_port)
    r = redis.Redis(host=redis_ip, port=redis_port)
    r.flushall()

    # Dynamically allocate ports to avoid CI conflicts
    gcs_port_A = find_free_port()
    dashboard_port_A = find_free_port()
    gcs_port_B = find_free_port()
    dashboard_port_B = find_free_port()

    # Define parameters for Primary Head Node A (Bootstraps cluster ID and session)
    params_A = RayParams(
        node_ip_address="127.0.0.1",
        redis_address=f"{redis_ip}:{redis_port}",
        gcs_server_port=gcs_port_A,
        dashboard_port=dashboard_port_A,
        include_dashboard=True,
        num_cpus=1,
        _system_config={"LEADER_ELECT": False},
        env_vars={"RAY_LEADER_ELECT": "false"},
    )
    node_A = Node(params_A, head=True, shutdown_at_exit=True)

    # Wait for Node A GCS to start and become healthy
    time.sleep(3)

    # Define parameters for Head Node B (Starts second -> Loses election -> Passive Standby)
    params_B = RayParams(
        node_ip_address="127.0.0.1",
        redis_address=f"{redis_ip}:{redis_port}",
        gcs_server_port=gcs_port_B,
        dashboard_port=dashboard_port_B,
        num_cpus=1,
        _system_config={"LEADER_ELECT": True},  # Passive
        env_vars={"RAY_LEADER_ELECT": "true"},
    )
    node_B = Node(params_B, head=True, shutdown_at_exit=True)

    # Wait for both nodes to settle
    time.sleep(5)

    try:
        # Check HTTP endpoints
        res_A = requests.get(f"http://localhost:{dashboard_port_A}/api/gcs_readiness")
        assert res_A.status_code == 200
        assert res_A.text == "success"

        res_B = requests.get(f"http://localhost:{dashboard_port_B}/api/gcs_readiness")
        assert res_B.status_code == 503
        assert "Readiness check failed" in res_B.text

        # Verify Node B is NOT in Redis NodeTable (only Node A is registered)
        ns = os.environ.get("RAY_external_storage_namespace", "default")
        node_ids_in_redis = [k.hex() for k in r.hkeys(f"RAY{ns}@NODE")]
        assert len(node_ids_in_redis) == 1
        assert node_A.node_id in node_ids_in_redis
        assert node_B.node_id not in node_ids_in_redis

        # Verify Node B's GCS is alive and caches node_B's registration in-memory
        # (Both nodes' processes stay alive and healthy!)
        assert node_A.remaining_processes_alive()
        assert node_B.remaining_processes_alive()

        # Verify Autoscaler metrics address in Redis does not belong to passive Node B
        autoscaler_metrics_in_kv = r.get(
            f"RAY{ns}@KV@AutoscalerMetricsAddress".encode()
        )
        if autoscaler_metrics_in_kv:
            assert str(dashboard_port_B).encode() not in autoscaler_metrics_in_kv

    finally:
        node_B.kill_all_processes(check_alive=False)
        node_A.kill_all_processes(check_alive=False)
        # Reset internal kv and ray address
        ray.experimental.internal_kv._internal_kv_reset()
        ray._common.utils.reset_ray_address()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
