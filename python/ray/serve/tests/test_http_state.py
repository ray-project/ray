import json
from unittest.mock import patch
import asyncio

import pytest
from typing import Any, List, Tuple

import ray
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


def _update_and_check_proxy_status(state: HTTPProxyState, status: HTTPProxyStatus):
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


def test_http_state_update_restarts_unhealthy_proxies(mock_get_all_node_ids):
    """Test the update method in HTTPState would kill and restart unhealthy proxies.

    Set up a HTTPProxyState with UNHEALTHY status. Calls the update method on the
    HTTPState object. Expects the unhealthy proxy being replaced by a new proxy with
    STARTING status. The unhealthy proxy state is also shutting down.
    """
    state = _make_http_state(HTTPOptions(location=DeploymentMode.HeadOnly))
    state._proxy_states[HEAD_NODE_ID] = _create_http_proxy_state(
        status=HTTPProxyStatus.UNHEALTHY
    )

    # Ensure before the update method is called, the status of the proxy is UNHEALTHY.
    assert state._proxy_states[HEAD_NODE_ID].status == HTTPProxyStatus.UNHEALTHY
    old_proxy_state = state._proxy_states[HEAD_NODE_ID]
    old_proxy = old_proxy_state.actor_handle

    def _update_state_and_check_proxy_status(
        _state: HTTPState,
        _status: HTTPProxyStatus,
    ):
        _state.update()
        return _state._proxy_states[HEAD_NODE_ID].status == _status

    # Continuously trigger update and wait for status to be changed to STARTING.
    wait_for_condition(
        condition_predictor=_update_state_and_check_proxy_status,
        _state=state,
        _status=HTTPProxyStatus.STARTING,
    )

    new_proxy = state._proxy_states[HEAD_NODE_ID].actor_handle

    # Ensure the old proxy is getting shutdown.
    assert old_proxy_state._shutting_down

    # Ensure the new proxy is completely different object than old proxy.
    assert new_proxy != old_proxy


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


def test_http_proxy_state_update_starting_ready_succeed():
    """Test calling update method on HTTPProxyState when the proxy state is STARTING and
    when the ready call succeeded.

    The proxy state started with STARTING. After update is called and ready call
    succeeded, the state will change to HEALTHY.
    """
    proxy_state = _create_http_proxy_state()

    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == HTTPProxyStatus.STARTING

    # Continuously trigger update and wait for status to be changed.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.HEALTHY,
    )


def test_http_proxy_state_update_starting_ready_failed_once():
    """Test calling update method on HTTPProxyState when the proxy state is STARTING and
    when the ready call failed once and succeeded for the following call.

    The proxy state started with STARTING status. After update is called for the first
    time and read call is blocked, the status is not changed to UNHEALTHY immediately
    and should stay as STARTING. The following ready call is unblocked and succeed. The
    status will then change to HEALTHY.
    """
    signal = SignalActor.remote()

    @ray.remote(num_cpus=0)
    class NewMockHTTPProxyActor:
        async def ready(self):
            await signal.wait.remote()
            return json.dumps(["mock_worker_id", "mock_log_file_path"])

        async def check_health(self):
            pass

    proxy_state = _create_http_proxy_state(proxy_actor_class=NewMockHTTPProxyActor)

    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == HTTPProxyStatus.STARTING

    # Trigger update. The status do not change even when ready call is blocked.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.STARTING,
    )

    # Unblock ready call, trigger update, and wait for status change to HEALTHY.
    signal.send.remote()
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.HEALTHY,
    )


def test_http_proxy_state_update_starting_ready_always_fails():
    """Test calling update method on HTTPProxyState when the proxy state is STARTING and
    when the ready call is always failing.

    The proxy state started with STARTING. After update is called, read call only throws
    exceptions. The state will eventually change to UNHEALTHY after all retries have
    exhausted.
    """

    @ray.remote(num_cpus=0)
    class NewMockHTTPProxyActor:
        async def ready(self):
            raise Exception("Never be ready")

        async def check_health(self):
            pass

    proxy_state = _create_http_proxy_state(proxy_actor_class=NewMockHTTPProxyActor)

    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == HTTPProxyStatus.STARTING

    # Continuously trigger update and wait for status to be changed.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.UNHEALTHY,
    )

    # Ensure the _consecutive_health_check_failures is correct
    assert proxy_state._consecutive_health_check_failures == 3


@patch("ray.serve._private.http_state.PROXY_READY_CHECK_TIMEOUT_S", 1)
def test_http_proxy_state_update_starting_ready_always_timeout():
    """Test calling update method on HTTPProxyState when the proxy state is STARTING and
    when the ready call always timed out.

    The proxy state started with STARTING. After update is called, ready calls takes
    very long time to finish. The state will eventually change to UNHEALTHY after all
    retries have exhausted.
    """

    @ray.remote(num_cpus=0)
    class NewMockHTTPProxyActor:
        async def ready(self):
            await asyncio.sleep(100)

        async def check_health(self):
            pass

    proxy_state = _create_http_proxy_state(proxy_actor_class=NewMockHTTPProxyActor)

    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == HTTPProxyStatus.STARTING

    # Continuously trigger update and wait for status to be changed.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.UNHEALTHY,
    )


@patch("ray.serve._private.http_state.PROXY_HEALTH_CHECK_PERIOD_S", 0.1)
def test_http_proxy_state_update_healthy_check_health_succeed():
    """Test calling update method on HTTPProxyState when the proxy state is HEALTHY and
    when the check_health call succeeded

    The proxy state started with HEALTHY. After update is called and ready call
    succeeded, the status will change to HEALTHY. After the next period of check_health
    call, the status should stay as HEALTHY.
    """
    proxy_state = _create_http_proxy_state()

    # Continuously trigger update. The status should change from STARTING to HEALTHY
    # when ready.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.HEALTHY,
    )
    first_check_time = proxy_state._last_health_check_time

    # Continuously trigger update and status continue to be HEALTHY.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.HEALTHY,
    )

    # Ensure the check time have changed since the last update
    assert first_check_time != proxy_state._last_health_check_time


@patch("ray.serve._private.http_state.PROXY_HEALTH_CHECK_PERIOD_S", 0.1)
def test_http_proxy_state_update_healthy_check_health_failed_once():
    """Test calling update method on HTTPProxyState when the proxy state is HEALTHY and
    when the check_health call failed once and succeeded for the following call.

    The proxy state started with STARTING. After update is called and ready call
    succeeded, the status will change to HEALTHY. After the next period of check_health
    call and that check_health call failed, the status should not be set to UNHEALTHY
    and should stay as HEALTHY. The following check_health call continue to succeed
    and the status continue to stay as HEALTHY.
    """
    signal = SignalActor.remote()

    @ray.remote(num_cpus=0)
    class NewMockHTTPProxyActor:
        async def ready(self):
            return json.dumps(["mock_worker_id", "mock_log_file_path"])

        async def check_health(self):
            await signal.wait.remote()

    proxy_state = _create_http_proxy_state(proxy_actor_class=NewMockHTTPProxyActor)

    # Continuously trigger update. The status should change from STARTING to HEALTHY
    # when ready.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.HEALTHY,
    )
    first_check_time = proxy_state._last_health_check_time

    # Continuously trigger update and status continue to be HEALTHY.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.HEALTHY,
    )

    # Ensure the check time have changed since the last update
    assert first_check_time != proxy_state._last_health_check_time

    # Unblock ready check, trigger update, and the status is still HEALTHY.
    signal.send.remote()
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.HEALTHY,
    )


@patch("ray.serve._private.http_state.PROXY_HEALTH_CHECK_PERIOD_S", 0.1)
def test_http_proxy_state_update_healthy_check_health_always_fails():
    """Test calling update method on HTTPProxyState when the proxy state is HEALTHY and
    when the check_health call is always failing.

    The proxy state started with STARTING. After update is called and ready call
    succeeded, the status will change to HEALTHY. After the next few check_health called
    and failed, the status will eventually change to UNHEALTHY after all retries have
    exhausted.
    """

    @ray.remote(num_cpus=0)
    class NewMockHTTPProxyActor:
        async def ready(self):
            return json.dumps(["mock_worker_id", "mock_log_file_path"])

        async def check_health(self):
            raise Exception("Never be healthy")

    proxy_state = _create_http_proxy_state(proxy_actor_class=NewMockHTTPProxyActor)

    # Continuously trigger update. The status should change from STARTING to HEALTHY
    # when ready.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.HEALTHY,
    )
    first_check_time = proxy_state._last_health_check_time

    # Continuously trigger update and status should change from HEALTHY to UNHEALTHY.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.UNHEALTHY,
    )

    # Ensure the check time have changed since the last update
    assert first_check_time != proxy_state._last_health_check_time

    # Ensure _consecutive_health_check_failures is correct
    assert proxy_state._consecutive_health_check_failures == 3


@patch("ray.serve._private.http_state.DEFAULT_HEALTH_CHECK_TIMEOUT_S", 0.1)
@patch("ray.serve._private.http_state.PROXY_HEALTH_CHECK_PERIOD_S", 0.1)
def test_http_proxy_state_update_healthy_check_health_always_timeout():
    """Test calling update method on HTTPProxyState when the proxy state is HEALTHY and
    when the ready call always timed out.

    The proxy state started with STARTING. After update is called and ready call
    succeeded, the status will change to HEALTHY. After the next few check_health calls
    takes very long time to finished, the status will eventually change to UNHEALTHY
    after all retries have exhausted.
    """

    @ray.remote(num_cpus=0)
    class NewMockHTTPProxyActor:
        async def ready(self):
            return json.dumps(["mock_worker_id", "mock_log_file_path"])

        async def check_health(self):
            await asyncio.sleep(100)

    proxy_state = _create_http_proxy_state(proxy_actor_class=NewMockHTTPProxyActor)

    # Continuously trigger update. The status should change from STARTING to HEALTHY
    # when ready.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.HEALTHY,
    )
    first_check_time = proxy_state._last_health_check_time

    # Continuously trigger update and status should change to UNHEALTHY.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.UNHEALTHY,
    )

    # Ensure the check time have changed since the last update
    assert first_check_time != proxy_state._last_health_check_time

    # Ensure _consecutive_health_check_failures is correct
    assert proxy_state._consecutive_health_check_failures == 3


@patch("ray.serve._private.http_state.PROXY_HEALTH_CHECK_PERIOD_S", 0.1)
def test_http_proxy_state_update_unhealthy_check_health_succeed():
    """Test calling update method on HTTPProxyState when the proxy state is UNHEALTHY
    and when the check_health call succeeded.

    The proxy state started with UNHEALTHY. After the next period of check_health
    call, the status changes to HEALTHY.
    """
    proxy_state = _create_http_proxy_state()
    proxy_state.set_status(status=HTTPProxyStatus.UNHEALTHY)

    # Ensure the proxy status is UNHEALTHY.
    assert proxy_state.status == HTTPProxyStatus.UNHEALTHY

    # Continuously trigger update and status should change to HEALTHY.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=HTTPProxyStatus.HEALTHY,
    )

    # Ensure _consecutive_health_check_failures has been reset
    assert proxy_state._consecutive_health_check_failures == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
