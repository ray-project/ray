import json
import time
from typing import List, Tuple
from unittest.mock import patch

import pytest

from ray._private.test_utils import wait_for_condition
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache
from ray.serve._private.common import ProxyStatus
from ray.serve._private.constants import (
    PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
    SERVE_CONTROLLER_NAME,
)
from ray.serve._private.proxy_state import (
    ProxyState,
    ProxyStateManager,
    ProxyWrapper,
    ProxyWrapperCallStatus,
)
from ray.serve._private.utils import Timer
from ray.serve.config import DeploymentMode, HTTPOptions
from ray.serve.tests.common.utils import MockTimer

HEAD_NODE_ID = "node_id-index-head"


class MockClusterNodeInfoCache:
    def __init__(self):
        self.alive_nodes = []

    def get_alive_nodes(self):
        return self.alive_nodes

    def get_alive_node_ids(self):
        return {node_id for node_id, _ in self.alive_nodes}


class FakeProxyActor:
    def __init__(self, *args, **kwargs):
        pass

    def ready(self):
        return json.dumps(["mock_worker_id", "mock_log_file_path"])

    def check_health(self):
        pass


class FakeProxyWrapper(ProxyWrapper):
    def __init__(self, *args, **kwargs):
        self.actor_handle = FakeProxyActor(*args, **kwargs)
        self.ready = ProxyWrapperCallStatus.FINISHED_SUCCEED
        self.health = ProxyWrapperCallStatus.FINISHED_SUCCEED
        self.worker_id = "mock_worker_id"
        self.log_file_path = "mock_log_file_path"
        self.health_check_ongoing = False
        self.is_draining = False
        self.shutdown = False
        self.num_health_checks = 0

    @property
    def actor_id(self) -> str:
        pass

    def reset_health_check(self):
        pass

    def start_new_ready_check(self):
        pass

    def start_new_health_check(self):
        self.health_check_ongoing = True

    def start_new_drained_check(self):
        pass

    def is_ready(self) -> ProxyWrapperCallStatus:
        return self.ready

    def is_healthy(self) -> ProxyWrapperCallStatus:
        self.num_health_checks += 1
        self.health_check_ongoing = False
        return self.health

    def is_drained(self) -> ProxyWrapperCallStatus:
        pass

    def is_shutdown(self):
        return self.shutdown

    def update_draining(self, draining: bool):
        pass

    def kill(self):
        self.shutdown = True

    def get_num_health_checks(self):
        return self.num_health_checks


def _create_proxy_state_manager(
    http_options: HTTPOptions = HTTPOptions(),
    head_node_id: str = HEAD_NODE_ID,
    cluster_node_info_cache=MockClusterNodeInfoCache(),
    actor_proxy_wrapper_class=FakeProxyWrapper,
    timer=Timer(),
) -> (ProxyStateManager, ClusterNodeInfoCache):
    return (
        ProxyStateManager(
            SERVE_CONTROLLER_NAME,
            config=http_options,
            head_node_id=head_node_id,
            cluster_node_info_cache=cluster_node_info_cache,
            actor_proxy_wrapper_class=actor_proxy_wrapper_class,
            timer=timer,
        ),
        cluster_node_info_cache,
    )


def _create_proxy_state(
    actor_proxy_wrapper_class=FakeProxyWrapper,
    status: ProxyStatus = ProxyStatus.STARTING,
    node_id: str = "mock_node_id",
    timer=Timer(),
    **kwargs,
) -> ProxyState:
    state = ProxyState(
        actor_proxy_wrapper=actor_proxy_wrapper_class(),
        actor_name="alice",
        node_id=node_id,
        node_ip="mock_node_ip",
        timer=timer,
    )
    state.set_status(status=status)
    return state


@pytest.fixture
def number_of_worker_nodes() -> int:
    return 100


@pytest.fixture
def all_nodes(number_of_worker_nodes) -> List[Tuple[str, str]]:
    return [(HEAD_NODE_ID, "fake-head-ip")] + [
        (f"worker-node-id-{i}", f"fake-worker-ip-{i}")
        for i in range(number_of_worker_nodes)
    ]


def _update_and_check_proxy_status(state: ProxyState, status: ProxyStatus):
    state.update()
    assert state.status == status, state.status
    return True


def _update_and_check_proxy_state_manager(
    proxy_state_manager: ProxyStateManager,
    node_ids: List[str],
    statuses: List[ProxyStatus],
    **kwargs,
):
    proxy_state_manager.update(**kwargs)
    proxy_states = proxy_state_manager._proxy_states
    assert all(
        [
            proxy_states[node_ids[idx]].status == statuses[idx]
            for idx in range(len(node_ids))
        ]
    ), [proxy_state.status for proxy_state in proxy_states.values()]
    return True


def test_node_selection(all_nodes):
    all_node_ids = {node_id for node_id, _ in all_nodes}
    # Test NoServer
    proxy_state_manager, cluster_node_info_cache = _create_proxy_state_manager(
        HTTPOptions(location=DeploymentMode.NoServer)
    )
    cluster_node_info_cache.alive_nodes = all_nodes
    assert proxy_state_manager._get_target_nodes(all_node_ids) == []

    # Test HeadOnly
    proxy_state_manager, cluster_node_info_cache = _create_proxy_state_manager(
        HTTPOptions(location=DeploymentMode.HeadOnly)
    )
    cluster_node_info_cache.alive_nodes = all_nodes
    assert proxy_state_manager._get_target_nodes(all_node_ids) == all_nodes[:1]

    # Test EveryNode
    proxy_state_manager, cluster_node_info_cache = _create_proxy_state_manager(
        HTTPOptions(location=DeploymentMode.EveryNode)
    )
    cluster_node_info_cache.alive_nodes = all_nodes
    assert proxy_state_manager._get_target_nodes(all_node_ids) == all_nodes

    # Test specific nodes
    proxy_state_manager, cluster_node_info_cache = _create_proxy_state_manager(
        HTTPOptions(location=DeploymentMode.EveryNode)
    )
    cluster_node_info_cache.alive_nodes = all_nodes
    assert proxy_state_manager._get_target_nodes({HEAD_NODE_ID}) == [
        (HEAD_NODE_ID, "fake-head-ip")
    ]


def test_proxy_state_update_restarts_unhealthy_proxies(all_nodes):
    """Test the update method in ProxyStateManager would
       kill and restart unhealthy proxies.

    Set up a ProxyState with UNHEALTHY status. Calls the update method on the
    ProxyStateManager object. Expects the unhealthy proxy being replaced
    by a new proxy with STARTING status.
    The unhealthy proxy state is also shutting down.
    """

    proxy_state_manager, cluster_node_info_cache = _create_proxy_state_manager()
    cluster_node_info_cache.alive_nodes = all_nodes
    proxy_state_manager.update()

    old_proxy_state = proxy_state_manager._proxy_states[HEAD_NODE_ID]
    old_proxy = old_proxy_state.actor_handle

    # Make the old proxy unhealthy.
    old_proxy_state.set_status(ProxyStatus.UNHEALTHY)

    # Continuously trigger update and wait for status to be changed to HEALTHY.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_state_manager,
        proxy_state_manager=proxy_state_manager,
        node_ids=[HEAD_NODE_ID],
        statuses=[ProxyStatus.HEALTHY],
    )

    new_proxy = proxy_state_manager._proxy_states[HEAD_NODE_ID].actor_handle

    # Ensure the old proxy is getting shutdown.
    assert old_proxy_state._shutting_down

    # Ensure the new proxy is completely different object than old proxy.
    assert new_proxy != old_proxy


def test_proxy_state_update_shutting_down():
    """Test calling update method on ProxyState when the proxy state is shutting
    down.

    This should be no-op. The status of the http proxy state will not be changed.
    """
    proxy_state = _create_proxy_state()
    previous_status = proxy_state.status
    proxy_state.shutdown()
    proxy_state.update()
    current_status = proxy_state.status

    # Ensure the proxy state is in the shutting down state.
    assert proxy_state._shutting_down

    # Ensure the status didn't change.
    assert previous_status == current_status


def test_proxy_state_update_starting_ready_succeed():
    """Test calling update method on ProxyState when the proxy state is STARTING and
    when the ready call succeeded.

    The proxy state started with STARTING. After update is called and ready call
    succeeded, the state will change to HEALTHY.
    """
    proxy_state = _create_proxy_state()

    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == ProxyStatus.STARTING

    # Ensure actor_details are set to the initial state when the proxy_state is created.
    assert proxy_state.actor_details.worker_id is None
    assert proxy_state.actor_details.log_file_path is None
    assert proxy_state.actor_details.status == ProxyStatus.STARTING.value

    # Continuously trigger update and wait for status to be changed.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.HEALTHY,
    )

    # Ensure actor_details are updated.
    assert proxy_state.actor_details.worker_id == "mock_worker_id"
    assert proxy_state.actor_details.log_file_path == "mock_log_file_path"
    assert proxy_state.actor_details.status == ProxyStatus.HEALTHY.value


def test_proxy_state_update_starting_ready_failed_once():
    """Test calling update method on ProxyState when the proxy state is STARTING and
    when the ready call failed once and succeeded for the following call.

    The proxy state started with STARTING status. After update is called for the first
    time and read call is blocked, the status is not changed to UNHEALTHY immediately
    and should stay as STARTING. The following ready call is unblocked and succeed. The
    status will then change to HEALTHY.
    """
    proxy_state = _create_proxy_state()

    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == ProxyStatus.STARTING

    # When the proxy ready call is blocked, the proxy wrapper call
    # status will be PENDING.
    proxy_state._actor_proxy_wrapper.ready = ProxyWrapperCallStatus.PENDING

    # Trigger update. The status do not change even when ready call is blocked.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.STARTING,
    )

    # Unblock ready call, trigger update, and wait for status change to HEALTHY.
    proxy_state._actor_proxy_wrapper.ready = ProxyWrapperCallStatus.FINISHED_SUCCEED
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.HEALTHY,
    )


def test_proxy_state_update_starting_ready_always_fails():
    """Test calling update method on ProxyState when the proxy state is STARTING and
    when the ready call is always failing.

    The proxy state started with STARTING. After update is called, read call only throws
    exceptions. The state will eventually change to UNHEALTHY after all retries have
    exhausted.
    """
    proxy_state = _create_proxy_state()
    # When the proxy ready call is failing, the proxy wrapper call
    # status will be FINISHED_FAILED.
    proxy_state._actor_proxy_wrapper.ready = ProxyWrapperCallStatus.FINISHED_FAILED

    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == ProxyStatus.STARTING

    # Continuously trigger update and wait for status to be changed.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.UNHEALTHY,
    )


@patch("ray.serve._private.proxy_state.PROXY_READY_CHECK_TIMEOUT_S", 0)
def test_proxy_state_update_starting_ready_always_timeout():
    """Test calling update method on ProxyState when the proxy state is STARTING and
    when the ready call always timed out.

    The proxy state started with STARTING. After update is called, ready calls takes
    very long time to finish. The state will eventually change to UNHEALTHY after all
    retries have exhausted.
    """
    proxy_state = _create_proxy_state()
    # When the proxy ready call always timeout, the proxy wrapper call
    # status will be PENDING.
    proxy_state._actor_proxy_wrapper.ready = ProxyWrapperCallStatus.PENDING

    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == ProxyStatus.STARTING

    # Continuously trigger update and wait for status to be changed.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.UNHEALTHY,
    )


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 0)
def test_proxy_state_update_healthy_check_health_succeed():
    """Test calling update method on ProxyState when the proxy state is HEALTHY and
    when the check_health call succeeded

    The proxy state started with HEALTHY. After update is called and ready call
    succeeded, the status will change to HEALTHY. After the next period of check_health
    call, the status should stay as HEALTHY.
    """
    proxy_state = _create_proxy_state()

    # Continuously trigger update. The status should change from STARTING to HEALTHY
    # when ready.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.HEALTHY,
    )
    first_check_time = proxy_state._last_health_check_time

    # Trigger update few more times and the status continue to be HEALTHY.
    for _ in range(3):
        _update_and_check_proxy_status(proxy_state, ProxyStatus.HEALTHY)
        time.sleep(0.1)

    # Ensure the check time have changed since the last update
    assert first_check_time != proxy_state._last_health_check_time


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 0)
def test_proxy_state_update_healthy_check_health_failed_once():
    """Test calling update method on ProxyState when the proxy state is HEALTHY and
    when the check_health call failed once and succeeded for the following call.

    The proxy state started with STARTING. After update is called and ready call
    succeeded, the status will change to HEALTHY. After the next period of check_health
    call and that check_health call failed, the status should not be set to UNHEALTHY
    and should stay as HEALTHY. The following check_health call continue to succeed
    and the status continue to stay as HEALTHY.
    """
    proxy_state = _create_proxy_state()
    proxy_state._actor_proxy_wrapper.health = ProxyWrapperCallStatus.FINISHED_FAILED

    # Continuously trigger update. The status should change from STARTING to HEALTHY
    # when ready.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.HEALTHY,
    )

    # Trigger update once and status continue to be HEALTHY.
    _update_and_check_proxy_status(proxy_state, ProxyStatus.HEALTHY)

    # Unblock health check, trigger update, and the status is still HEALTHY.
    proxy_state._actor_proxy_wrapper.health = ProxyWrapperCallStatus.FINISHED_SUCCEED
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.HEALTHY,
    )


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 0)
def test_proxy_state_update_healthy_check_health_always_fails():
    """Test calling update method on ProxyState when the proxy state is HEALTHY and
    when the check_health call is always failing.

    The proxy state started with STARTING. After update is called and ready call
    succeeded, the status will change to HEALTHY. After the next few check_health called
    and failed, the status will eventually change to UNHEALTHY after all retries have
    exhausted.
    """

    proxy_state = _create_proxy_state()
    # When the proxy health call is failing, the proxy wrapper call
    # status will be FINISHED_FAILED.
    proxy_state._actor_proxy_wrapper.health = ProxyWrapperCallStatus.FINISHED_FAILED

    # Continuously trigger update. The status should change from STARTING to HEALTHY
    # when ready.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.HEALTHY,
    )
    first_check_time = proxy_state._last_health_check_time

    # Continuously trigger update and status should change from HEALTHY to UNHEALTHY.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.UNHEALTHY,
    )

    # Ensure the check time have changed since the last update
    assert first_check_time != proxy_state._last_health_check_time

    # Ensure _consecutive_health_check_failures is correct
    assert proxy_state._consecutive_health_check_failures == 3


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 0)
def test_proxy_state_update_healthy_check_health_sometimes_fails():
    """Test that the proxy is UNHEALTHY after consecutive health-check failures.

    The proxy state starts with STARTING. Then the proxy fails a few times
    (less than the threshold needed to set it UNHEALTHY). Then it succeeds, so
    it becomes HEALTHY. Then it fails a few times again but stays HEALTHY
    because the failures weren't consecutive with the previous ones. And then
    it finally fails enough times to become UNHEALTHY.
    """

    proxy_state = _create_proxy_state()

    # Wait for the proxy to become ready.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.HEALTHY,
    )

    def _update_until_num_health_checks_received(
        state: ProxyState, num_health_checks: int
    ):
        state.update()
        assert (state._actor_proxy_wrapper.get_num_health_checks()) == num_health_checks
        return True

    def incur_health_checks(
        pass_checks: bool, num_checks: int, expected_final_status: ProxyStatus
    ):
        """Waits for num_checks health checks to occur.

        Args:
            pass_checks: whether the health checks should pass.
            num_checks: number of checks to wait for.
            expected_final_status: the final status that should be asserted.
        """
        if pass_checks:
            proxy_state._actor_proxy_wrapper.health = (
                ProxyWrapperCallStatus.FINISHED_SUCCEED
            )
        else:
            proxy_state._actor_proxy_wrapper.health = (
                ProxyWrapperCallStatus.FINISHED_FAILED
            )

        cur_num_health_checks = proxy_state._actor_proxy_wrapper.get_num_health_checks()

        wait_for_condition(
            condition_predictor=_update_until_num_health_checks_received,
            state=proxy_state,
            num_health_checks=cur_num_health_checks + num_checks,
        )
        assert (
            proxy_state._actor_proxy_wrapper.get_num_health_checks()
            <= cur_num_health_checks + num_checks
        )

        if expected_final_status:
            assert proxy_state.status == expected_final_status

    # Make sure that the proxy_state's status remains HEALTHY as long as
    # PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD failures don't occur consecutively.
    for _ in range(3):
        incur_health_checks(
            pass_checks=True,
            num_checks=1,
            expected_final_status=ProxyStatus.HEALTHY,
        )
        incur_health_checks(
            pass_checks=False,
            num_checks=PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD - 1,
            expected_final_status=ProxyStatus.HEALTHY,
        )

    # Have health check succeed one more time.
    incur_health_checks(
        pass_checks=True, num_checks=1, expected_final_status=ProxyStatus.HEALTHY
    )

    # Check failing the health check PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD
    # times makes the proxy UNHEALTHY again.
    incur_health_checks(
        pass_checks=False,
        num_checks=PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
        expected_final_status=ProxyStatus.UNHEALTHY,
    )


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_TIMEOUT_S", 0)
@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 0)
def test_proxy_state_check_health_always_timeout():
    """Test calling update method on ProxyState when the proxy state is HEALTHY and
    when the ready call always timed out and health check timeout and period equals.

    The proxy state started with STARTING. After update is called and ready call
    succeeded, the status will change to HEALTHY. After the next few check_health calls
    never finishes and always pending, the status will eventually change to UNHEALTHY
    after all retries have exhausted.
    """

    proxy_state = _create_proxy_state()
    proxy_state._actor_proxy_wrapper.health = ProxyWrapperCallStatus.PENDING

    # Continuously trigger update. The status should change from STARTING to HEALTHY
    # when ready.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.HEALTHY,
    )
    first_check_time = proxy_state._last_health_check_time

    # Continuously trigger update and status should change to UNHEALTHY.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.UNHEALTHY,
    )

    # Ensure the check time have changed since the last update
    assert first_check_time != proxy_state._last_health_check_time

    # Ensure _consecutive_health_check_failures is correct
    assert proxy_state._consecutive_health_check_failures == 3


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 0)
def test_proxy_state_update_unhealthy_check_health_succeed():
    """Test calling update method on ProxyState when the proxy state has
    failed health checks and the next check_health call succeeded.

    The proxy state started with STARTING. After the next period of check_health
    call, the status changes to HEALTHY.
    """
    proxy_state = _create_proxy_state()
    proxy_state._consecutive_health_check_failures = 1

    assert proxy_state.status == ProxyStatus.STARTING

    # Continuously trigger update and status should change to HEALTHY.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.HEALTHY,
    )

    # Ensure _consecutive_health_check_failures has been reset
    assert proxy_state._consecutive_health_check_failures == 0


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_TIMEOUT_S", 0)
@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 0)
def test_unhealthy_retry_correct_number_of_times():
    """Test the unhealthy retry logic retires the correct number of times.

    When the health check fails 3 times (default retry threshold), the proxy state
    should change from HEALTHY to UNHEALTHY.
    """

    proxy_state = _create_proxy_state()
    proxy_state._actor_proxy_wrapper.health = ProxyWrapperCallStatus.PENDING

    # Trigger update once. The status should change from STARTING to HEALTHY
    proxy_state.update()
    assert proxy_state.status == ProxyStatus.HEALTHY

    # 3 consecutive failures should trigger the proxy state to be UNHEALTHY
    def proxy_state_consecutive_health_check_failures(num_failures):
        proxy_state.update()
        assert proxy_state._consecutive_health_check_failures == num_failures
        return True

    wait_for_condition(
        condition_predictor=proxy_state_consecutive_health_check_failures,
        num_failures=3,
    )

    assert proxy_state.status == ProxyStatus.UNHEALTHY


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 0)
@pytest.mark.parametrize("number_of_worker_nodes", [0, 1, 2, 3])
def test_update_draining(all_nodes, number_of_worker_nodes):
    """Test update draining logics.

    When update nodes to inactive, head node http proxy should never be draining while
    worker node http proxy should change to draining. When update nodes to active, head
    node http proxy should continue to be healthy while worker node http proxy should
    be healthy.
    """
    manager, cluster_node_info_cache = _create_proxy_state_manager(
        HTTPOptions(location=DeploymentMode.EveryNode)
    )
    cluster_node_info_cache.alive_nodes = all_nodes

    for node_id, _ in all_nodes:
        manager._proxy_states[node_id] = _create_proxy_state(
            status=ProxyStatus.HEALTHY,
            node_id=node_id,
        )
    node_ids = [node_id for node_id, _ in all_nodes]

    # No target proxy nodes
    proxy_nodes = set()

    # Head node proxy should continue to be HEALTHY.
    # Worker node proxy should turn DRAINING.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_state_manager,
        proxy_state_manager=manager,
        node_ids=node_ids,
        statuses=[ProxyStatus.HEALTHY]
        + [ProxyStatus.DRAINING] * number_of_worker_nodes,
        proxy_nodes=proxy_nodes,
    )

    # All nodes are target proxy nodes
    proxy_nodes = set(node_ids)

    # Head node proxy should continue to be HEALTHY.
    # Worker node proxy should turn HEALTHY.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_state_manager,
        proxy_state_manager=manager,
        node_ids=node_ids,
        statuses=[ProxyStatus.HEALTHY] * (number_of_worker_nodes + 1),
        proxy_nodes=proxy_nodes,
    )


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 0)
def test_proxy_actor_healthy_during_draining():
    """Test that the proxy will remain DRAINING even if health check succeeds."""

    proxy_state = _create_proxy_state()

    # Wait for the proxy to become ready.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.HEALTHY,
    )

    # Drain the proxy.
    proxy_state.update(draining=True)
    assert proxy_state.status == ProxyStatus.DRAINING

    cur_num_health_checks = proxy_state._actor_proxy_wrapper.get_num_health_checks()

    def _update_until_two_more_health_checks():
        # Check 2 more health checks to make sure the proxy state
        # at least sees the first successful health check.
        proxy_state.update(draining=True)
        return (
            proxy_state._actor_proxy_wrapper.get_num_health_checks()
            == cur_num_health_checks + 2
        )

    wait_for_condition(_update_until_two_more_health_checks)

    # Make sure the status is still DRAINING not HEALTHY
    assert proxy_state.status == ProxyStatus.DRAINING


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 0)
@patch("ray.serve._private.proxy_state.PROXY_DRAIN_CHECK_PERIOD_S", 0)
@pytest.mark.parametrize("number_of_worker_nodes", [1])
def test_proxy_actor_unhealthy_during_draining(all_nodes, number_of_worker_nodes):
    """Test the state transition from DRAINING to UNHEALTHY for the proxy actor."""
    manager, cluster_node_info_cache = _create_proxy_state_manager(
        HTTPOptions(location=DeploymentMode.EveryNode)
    )
    cluster_node_info_cache.alive_nodes = all_nodes

    worker_node_id = None
    for node_id, node_ip_address in all_nodes:
        manager._proxy_states[node_id] = _create_proxy_state(
            status=ProxyStatus.STARTING,
            node_id=node_id,
        )
        if node_id != HEAD_NODE_ID:
            worker_node_id = node_id

    node_ids = [node_id for node_id, _ in all_nodes]

    # All nodes are target proxy nodes
    proxy_nodes = set(node_ids)
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_state_manager,
        proxy_state_manager=manager,
        node_ids=node_ids,
        statuses=[ProxyStatus.HEALTHY] * (number_of_worker_nodes + 1),
        proxy_nodes=proxy_nodes,
    )

    # No target proxy nodes
    proxy_nodes = set()

    # Head node proxy should continue to be HEALTHY.
    # Worker node proxy should turn DRAINING.
    wait_for_condition(
        condition_predictor=_update_and_check_proxy_state_manager,
        proxy_state_manager=manager,
        node_ids=node_ids,
        statuses=[ProxyStatus.HEALTHY]
        + [ProxyStatus.DRAINING] * number_of_worker_nodes,
        proxy_nodes=proxy_nodes,
    )

    # Kill the draining proxy actor
    manager._proxy_states[
        worker_node_id
    ]._actor_proxy_wrapper.health = ProxyWrapperCallStatus.FINISHED_FAILED

    def check_worker_node_proxy_actor_is_removed():
        manager.update(proxy_nodes=proxy_nodes)
        return len(manager._proxy_states) == 1

    wait_for_condition(condition_predictor=check_worker_node_proxy_actor_is_removed)
    assert manager._proxy_states[HEAD_NODE_ID].status == ProxyStatus.HEALTHY


def test_is_ready_for_shutdown(all_nodes):
    """Test `is_ready_for_shutdown()` returns True the correct state.

    Before `shutdown()` is called, `is_ready_for_shutdown()` should return false. After
    `shutdown()` is called and all proxy actor are killed, `is_ready_for_shutdown()`
    should return true.
    """
    manager, cluster_node_info_cache = _create_proxy_state_manager(
        HTTPOptions(location=DeploymentMode.EveryNode)
    )
    cluster_node_info_cache.alive_nodes = all_nodes

    for node_id, node_ip_address in all_nodes:
        manager._proxy_states[node_id] = _create_proxy_state(
            status=ProxyStatus.HEALTHY,
            node_id=node_id,
        )

    # Ensure before shutdown, manager is not shutdown
    assert not manager.is_ready_for_shutdown()

    manager.shutdown()

    # Ensure after shutdown, manager is shutdown and all proxy states are shutdown
    def check_is_ready_for_shutdown():
        return manager.is_ready_for_shutdown()

    wait_for_condition(check_is_ready_for_shutdown)


@patch("ray.serve._private.proxy_state.PROXY_READY_CHECK_TIMEOUT_S", 0.1)
@pytest.mark.parametrize("number_of_worker_nodes", [1])
def test_proxy_starting_timeout_longer_than_env(number_of_worker_nodes, all_nodes):
    """Test update method on ProxyStateManager when the proxy state is STARTING and
    when the ready call takes longer than PROXY_READY_CHECK_TIMEOUT_S.

    The proxy state started with STARTING. After update is called, ready calls takes
    some time to finish. The proxy state manager will restart the proxy state after
    PROXY_READY_CHECK_TIMEOUT_S. After the next period of check_health call,
    the proxy state manager will check on backoff timeout, not immediately
    restarting the proxy states, and eventually set the proxy state to HEALTHY.
    """
    fake_time = MockTimer()
    proxy_state_manager, cluster_node_info_cache = _create_proxy_state_manager(
        http_options=HTTPOptions(location=DeploymentMode.EveryNode),
        timer=fake_time,
    )
    cluster_node_info_cache.alive_nodes = all_nodes

    node_ids = {node[0] for node in all_nodes}

    # Run update to create proxy states.
    proxy_state_manager.update(proxy_nodes=node_ids)
    old_proxy_states = {
        node_id: state for node_id, state in proxy_state_manager._proxy_states.items()
    }

    # Ensure 2 proxies are created, one for the head node and another for the worker.
    assert len(proxy_state_manager._proxy_states) == len(node_ids)

    # Ensure the proxy state statuses before update are STARTING. Also, setting the
    # ready call status to be pending to simulate the call never respond.
    def check_proxy_state_starting(_proxy_state_manager: ProxyStateManager):
        for proxy_state in _proxy_state_manager._proxy_states.values():
            assert proxy_state.status == ProxyStatus.STARTING
            proxy_state._actor_proxy_wrapper.ready = ProxyWrapperCallStatus.PENDING

    # Trigger update and ensure proxy states are restarted due to time advanced
    # longer than PROXY_READY_CHECK_TIMEOUT_S of 0.1s.
    check_proxy_state_starting(_proxy_state_manager=proxy_state_manager)
    fake_time.advance(0.11)
    proxy_state_manager.update(proxy_nodes=node_ids)
    assert all(
        [
            proxy_state_manager._proxy_states[node_id] != old_proxy_states[node_id]
            for node_id in node_ids
        ]
    )

    # Trigger another update with the same advance time and this time the
    # proxy states should not be restarted again with the backoff timeout.
    old_proxy_states = {
        node_id: state for node_id, state in proxy_state_manager._proxy_states.items()
    }
    check_proxy_state_starting(_proxy_state_manager=proxy_state_manager)
    fake_time.advance(0.11)
    proxy_state_manager.update(proxy_nodes=node_ids)
    assert all(
        [
            proxy_state_manager._proxy_states[node_id] == old_proxy_states[node_id]
            for node_id in node_ids
        ]
    )

    # Ensure the proxy states turns healthy after the ready call is unblocked.
    for proxy_state in proxy_state_manager._proxy_states.values():
        proxy_state._actor_proxy_wrapper.ready = ProxyWrapperCallStatus.FINISHED_SUCCEED
    proxy_state_manager.update(proxy_nodes=node_ids)
    assert all(
        [
            proxy_state_manager._proxy_states[node_id].status == ProxyStatus.HEALTHY
            for node_id in node_ids
        ]
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
