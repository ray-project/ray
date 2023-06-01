import json
from unittest.mock import Mock, patch
import asyncio

import pytest
from typing import Any, List, Tuple

import ray
from ray.experimental.state.api import list_actors
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve.config import DeploymentMode, HTTPOptions
from ray.serve._private.common import HTTPProxyStatus
from ray.serve._private.http_state import HTTPState, HTTPProxyState

HEAD_NODE_ID = "node_id-index-head"


def _make_http_state(
    http_options: HTTPOptions,
    head_node_id: str = HEAD_NODE_ID,
) -> HTTPState:
    return HTTPState(
        "mock_controller_name",
        detached=True,
        config=http_options,
        head_node_id=head_node_id,
        gcs_client=None,
        _start_proxies_on_init=False,
    )


@pytest.fixture
def all_nodes() -> List[Tuple[str, str]]:
    return [(HEAD_NODE_ID, "fake-head-ip")] + [
        (f"worker-node-id-{i}", f"fake-worker-ip-{i}") for i in range(100)
    ]


@pytest.fixture()
def mock_get_all_node_ids(all_nodes):
    with patch("ray.serve._private.http_state.get_all_node_ids") as func:
        func.return_value = all_nodes
        yield


@ray.remote(num_cpus=0)
class MockHTTPProxyActor:
    async def ready(self):
        return json.dumps(["mock_worker_id", "mock_log_file_path"])

    async def check_health(self):
        pass


def _create_http_proxy_state(
    proxy_actor_class: Any = MockHTTPProxyActor,
    status: HTTPProxyStatus = HTTPProxyStatus.STARTING,
) -> HTTPProxyState:
    proxy = proxy_actor_class.options(lifetime="detached").remote()
    state = HTTPProxyState(proxy, "alice", "mock_node_id", "mock_node_ip")
    state.set_status(status=status)
    print(f"The http proxy state created has the status of: {state.status}")
    return state


def _check_proxy(state: HTTPProxyState, status: HTTPProxyStatus):
    state.update()
    return state.status == status


def test_node_selection(all_nodes, mock_get_all_node_ids):
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

    # @ray.remote(num_cpus=0)
    # class MockHTTPProxyActor:
    #     async def ready(self):
    #         await signal.wait.remote()
    #         return json.dumps(["mock_worker_id", "mock_log_file_path"])
    #
    #     async def check_health(self):
    #         pass

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


def test_http_state_update_restarts_unhealthy_proxies(mock_get_all_node_ids):
    """Test the update method in HTTPState would kill and restart an unhealthy proxy.

    Setting up the http proxy state with UNHEALTHY status and calls the update method on
    the HTTPState object should expect the unhealthy proxy being replaced by a new proxy
    with a STARTING status. Also, the unhealthy proxy state should be showdown.
    """
    state = _make_http_state(HTTPOptions(location=DeploymentMode.HeadOnly))
    state._proxy_states[HEAD_NODE_ID] = _create_http_proxy_state(
        status=HTTPProxyStatus.UNHEALTHY
    )

    # Ensure before the update method is called, the status of the proxy is UNHEALTHY.
    assert state._proxy_states[HEAD_NODE_ID].status == HTTPProxyStatus.UNHEALTHY
    old_proxy_state = state._proxy_states[HEAD_NODE_ID]
    old_proxy = old_proxy_state.actor_handle
    state.update()

    # Ensure after the update method is called, the status of the proxy is STARTING.
    assert state._proxy_states[HEAD_NODE_ID].status == HTTPProxyStatus.STARTING
    new_proxy = state._proxy_states[HEAD_NODE_ID].actor_handle

    # Ensure the old proxy is getting shutdown.
    assert old_proxy_state._shutting_down

    # Ensure the new proxy is completely different instance than  old proxy.
    assert old_proxy != new_proxy


def test_http_proxy_state_update_shutting_down():
    """Test calling update method on HTTPProxyState when the proxy state is shutting
    down.

    This should be no-op. The status of the http proxy state will not be changed.
    """
    proxy_state = _create_http_proxy_state()
    previous_status = proxy_state.status
    proxy_state.shutdown()
    proxy_state.update()
    current_status = proxy_state.status

    # Ensure the proxy state is in the shutting down state.
    assert proxy_state._shutting_down

    # Ensure the status didn't change.
    assert previous_status == current_status


def test_http_proxy_state_update_starting_ready_successful():
    """Test calling update method on HTTPProxyState when the proxy state is STARTING and
    when the ready call succeeded

    The proxy state started with STARTING. After update is called and ready call
    succeeded, the state will change to HEALTHY
    """
    proxy_state = _create_http_proxy_state()

    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == HTTPProxyStatus.STARTING

    # Trigger update and wait for status to be changed.
    proxy_state.update()
    wait_for_condition(
        condition_predictor=_check_proxy,
        state=proxy_state,
        status=HTTPProxyStatus.HEALTHY,
        timeout=2,
    )

    # Ensure the proxy status is changed to HEALTHY after update.
    assert proxy_state.status == HTTPProxyStatus.HEALTHY


def test_http_proxy_state_update_starting_ready_exception_once():
    """Test calling update method on HTTPProxyState when the proxy state is"""
    mock_ready = Mock(
        side_effect=[
            KeyError("bar"),
            json.dumps(["mock_worker_id", "mock_log_file_path"]),
        ]
    )
    proxy_state = _create_http_proxy_state()
    proxy_state._ready_obj_ref = mock_ready

    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == HTTPProxyStatus.STARTING

    # Trigger update and wait for status to be changed.
    proxy_state.update()
    wait_for_condition(
        condition_predictor=_check_proxy,
        state=proxy_state,
        status=HTTPProxyStatus.HEALTHY,
        timeout=2,
    )

    # Ensure the proxy status is changed to HEALTHY after update.
    assert proxy_state.status == HTTPProxyStatus.HEALTHY


def test_http_proxy_state_update_starting_ready_exception_many_times():
    """Test calling update method on HTTPProxyState when the proxy state is"""
    pass


def test_http_proxy_state_update_starting_ready_timeout_once():
    """Test calling update method on HTTPProxyState when the proxy state is"""
    pass


def test_http_proxy_state_update_starting_ready_timeout_many_times():
    """Test calling update method on HTTPProxyState when the proxy state is"""
    pass


# TODO: fill more test cases


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
