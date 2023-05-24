import json
from unittest.mock import patch
import asyncio

import pytest

import ray
from ray.experimental.state.api import list_actors
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve.config import DeploymentMode, HTTPOptions
from ray.serve._private.common import HTTPProxyStatus
from ray.serve._private.http_state import HTTPState, HTTPProxyState


def test_node_selection():
    head_node_id = "node_id-index-head"

    def _make_http_state(http_options):
        return HTTPState(
            "mock_controller_name",
            detached=True,
            config=http_options,
            head_node_id=head_node_id,
            gcs_client=None,
            _start_proxies_on_init=False,
        )

    all_nodes = [(head_node_id, "fake-head-ip")] + [
        (f"worker-node-id-{i}", f"fake-worker-ip-{i}") for i in range(100)
    ]

    with patch("ray.serve._private.http_state.get_all_node_ids") as func:
        func.return_value = all_nodes

        # Test NoServer
        state = _make_http_state(HTTPOptions(location=DeploymentMode.NoServer))
        assert state._get_target_nodes() == []

        # Test HeadOnly
        state = _make_http_state(HTTPOptions(location=DeploymentMode.HeadOnly))
        assert state._get_target_nodes() == all_nodes[:1]

        # Test EveryNode
        state = _make_http_state(HTTPOptions(location=DeploymentMode.EveryNode))
        assert state._get_target_nodes() == all_nodes

        # Test FixedReplica
        state = _make_http_state(
            HTTPOptions(location=DeploymentMode.FixedNumber, fixed_number_replicas=5)
        )
        selected_nodes = state._get_target_nodes()

        # it should have selection a subset of 5 nodes.
        assert len(selected_nodes) == 5
        assert set(all_nodes).issuperset(set(selected_nodes))

        for _ in range(5):
            # The selection should be deterministic.
            assert selected_nodes == state._get_target_nodes()

        another_seed = _make_http_state(
            HTTPOptions(
                location=DeploymentMode.FixedNumber,
                fixed_number_replicas=5,
                fixed_number_selection_seed=42,
            )
        )._get_target_nodes()
        assert len(another_seed) == 5
        assert set(all_nodes).issuperset(set(another_seed))
        assert set(another_seed) != set(selected_nodes)


def test_http_proxy_healthy():
    ray.init()
    signal = SignalActor.remote()

    @ray.remote(num_cpus=0)
    class MockHTTPProxyActor:
        async def ready(self):
            await signal.wait.remote()
            return json.dumps(["mock_worker_id", "mock_log_file_path"])

        async def check_health(self):
            pass

    proxy = MockHTTPProxyActor.options(lifetime="detached").remote()
    state = HTTPProxyState(proxy, "alice", "mock_node_id", "mock_node_ip")
    assert state.status == HTTPProxyStatus.STARTING

    state.update()
    assert state.status == HTTPProxyStatus.STARTING

    signal.send.remote()

    def check_proxy(status):
        state.update()
        return state.status == status

    wait_for_condition(check_proxy, status=HTTPProxyStatus.HEALTHY, timeout=2)
    ray.shutdown()


def test_http_proxy_unhealthy():
    ray.init()
    signal = SignalActor.remote()

    @ray.remote(num_cpus=0)
    class MockHTTPProxyActor:
        async def ready(self):
            return json.dumps(["mock_worker_id", "mock_log_file_path"])

        async def check_health(self):
            await signal.wait.remote()

    with patch("ray.serve._private.http_state.PROXY_HEALTH_CHECK_PERIOD_S", 1):
        proxy = MockHTTPProxyActor.options(lifetime="detached").remote()
        state = HTTPProxyState(proxy, "alice", "mock_node_id", "mock_node_ip")
        assert state.status == HTTPProxyStatus.STARTING

        def check_proxy(status):
            state.update()
            return state.status == status

        # Proxy actor is ready, so status should transition STARTING -> HEALTHY
        wait_for_condition(check_proxy, status=HTTPProxyStatus.HEALTHY, timeout=2)

        # Health check is blocked, so status should transition HEALTHY -> UNHEALTHY
        wait_for_condition(check_proxy, status=HTTPProxyStatus.UNHEALTHY, timeout=2)

        # Unblock health check, so status should transition UNHEALTHY -> HEALTHY
        signal.send.remote()
        wait_for_condition(check_proxy, status=HTTPProxyStatus.HEALTHY, timeout=2)

    ray.shutdown()


def test_http_proxy_shutdown():
    ray.init()

    @ray.remote(num_cpus=0)
    class MockHTTPProxyActor:
        async def ready(self):
            return json.dumps(["mock_worker_id", "mock_log_file_path"])

        async def check_health(self):
            await asyncio.sleep(100)

    proxy = MockHTTPProxyActor.options(lifetime="detached").remote()
    state = HTTPProxyState(proxy, "alice", "mock_node_id", "mock_node_ip")
    assert state.status == HTTPProxyStatus.STARTING

    def check_proxy(status):
        state.update()
        return state.status == status

    # Proxy actor is ready, so status should transition STARTING -> HEALTHY
    wait_for_condition(check_proxy, status=HTTPProxyStatus.HEALTHY, timeout=2)

    # Confirm that a new health check has been started
    state.update()
    assert state._health_check_obj_ref

    # Shutdown the http proxy state. Wait for the http proxy actor to be killed
    state.shutdown()
    wait_for_condition(lambda: len(list_actors(filters=[("state", "=", "ALIVE")])) == 0)

    # Make sure that the state doesn't try to check on the status of the dead actor
    state.update()
    assert state.status == HTTPProxyStatus.HEALTHY

    ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
