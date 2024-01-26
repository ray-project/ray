import json
from typing import List, Optional, Tuple
from unittest.mock import patch

import pytest

from ray._private.test_utils import wait_for_condition
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache
from ray.serve._private.common import ProxyStatus
from ray.serve._private.constants import PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD
from ray.serve._private.proxy_state import ProxyState, ProxyStateManager, ProxyWrapper
from ray.serve._private.test_utils import MockTimer
from ray.serve._private.utils import Timer
from ray.serve.config import DeploymentMode, HTTPOptions
from ray.serve.schema import LoggingConfig

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
        self.is_ready_response = None
        self.is_healthy_response = None
        self.is_drained_response = False
        self.worker_id = "mock_worker_id"
        self.log_file_path = "mock_log_file_path"
        self.shutdown = False
        self.num_health_checks = 0
        self.num_drain_checks = 0

    @property
    def actor_id(self) -> str:
        pass

    def is_ready(self, timeout_s: float) -> Optional[bool]:
        return self.is_ready_response

    def is_healthy(self, timeout_s: float) -> Optional[bool]:
        self.num_health_checks += 1
        return self.is_healthy_response

    def is_drained(self, timeout_s: float) -> Optional[bool]:
        self.num_drain_checks += 1
        return self.is_drained_response

    def is_shutdown(self):
        return self.shutdown

    def update_draining(self, draining: bool):
        pass

    def kill(self):
        self.shutdown = True

    def get_num_health_checks(self):
        return self.num_health_checks

    def get_num_drain_checks(self):
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
            config=http_options,
            head_node_id=head_node_id,
            cluster_node_info_cache=cluster_node_info_cache,
            logging_config=LoggingConfig(),
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
    state._set_status(status=status)
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


def _reconcile_and_check_proxy_status(state: ProxyState, status: ProxyStatus):
    state.reconcile()
    assert state.status == status
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


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 5)
def test_proxy_state_manager_restarts_unhealthy_proxies(all_nodes):
    """Test the update method in ProxyStateManager would
    kill and restart unhealthy proxies.
    """

    timer = MockTimer()
    proxy_state_manager, cluster_node_info_cache = _create_proxy_state_manager(
        timer=timer
    )

    cluster_node_info_cache.alive_nodes = all_nodes
    # First iteration, refresh state
    proxy_state_manager.update()

    prev_proxy_state = proxy_state_manager._proxy_states[HEAD_NODE_ID]
    # Mark existing head-node proxy UNHEALTHY
    prev_proxy_state._set_status(ProxyStatus.UNHEALTHY)
    old_proxy = prev_proxy_state.actor_handle

    # Continuously trigger update and wait for status to be changed to HEALTHY.
    for _ in range(1):
        proxy_state_manager.update(proxy_nodes=set(HEAD_NODE_ID))
        # Advance timer by 5 (to perform a health-check)
        timer.advance(5)

    new_proxy_state = proxy_state_manager._proxy_states[HEAD_NODE_ID]
    # Previous proxy's state stays UNHEALTHY
    assert prev_proxy_state.status == ProxyStatus.UNHEALTHY
    # Ensure the old proxy is getting shutdown.
    assert prev_proxy_state._shutting_down
    # New proxy's state should be STARTING
    assert new_proxy_state.status == ProxyStatus.STARTING
    assert new_proxy_state.proxy_restart_count == 1

    new_proxy = new_proxy_state.actor_handle
    # Ensure the new proxy is completely different object than old proxy.
    assert new_proxy != old_proxy


def test_proxy_state_reconcile_shutting_down():
    proxy_state = _create_proxy_state()
    previous_status = proxy_state.status
    proxy_state.shutdown()
    # This should be no-op. The status of the http proxy state will not be changed.
    proxy_state.reconcile()
    current_status = proxy_state.status

    # Ensure the proxy state is in the shutting down state.
    assert proxy_state._shutting_down

    # Ensure the status didn't change.
    assert previous_status == current_status


def test_proxy_state_reconcile_readiness_check_succeed():
    proxy_state = _create_proxy_state()

    # Configure is_ready to be true
    proxy_state._actor_proxy_wrapper.is_ready_response = True

    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == ProxyStatus.STARTING

    # Ensure actor_details are set to the initial state when the proxy_state is created.
    assert proxy_state.actor_details.worker_id is None
    assert proxy_state.actor_details.log_file_path is None
    assert proxy_state.actor_details.status == ProxyStatus.STARTING.value

    # Continuously trigger update and wait for status to be changed.
    proxy_state.reconcile()
    assert proxy_state.status == ProxyStatus.HEALTHY

    # Ensure actor_details are updated.
    assert proxy_state.actor_details.worker_id == "mock_worker_id"
    assert proxy_state.actor_details.log_file_path == "mock_log_file_path"
    assert proxy_state.actor_details.status == ProxyStatus.HEALTHY.value


def test_proxy_state_reconcile_readiness_check_pending():
    proxy_state = _create_proxy_state()

    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == ProxyStatus.STARTING

    # When the proxy readiness check is pending, the proxy wrapper is_ready
    # will return None
    proxy_state._actor_proxy_wrapper.is_ready_response = None

    # Trigger update. The status do not change, while readiness check is pending
    for _ in range(10):
        proxy_state.reconcile()
        assert proxy_state.status == ProxyStatus.STARTING

    # Unblock is_ready call, trigger update, and wait for status change to HEALTHY.
    proxy_state._actor_proxy_wrapper.is_ready_response = True

    proxy_state.reconcile()
    assert proxy_state.status == ProxyStatus.HEALTHY


def test_proxy_state_reconcile_readiness_check_fails():
    proxy_state = _create_proxy_state()

    # Emulate readiness check failure
    proxy_state._actor_proxy_wrapper.is_ready_response = False
    # Ensure the proxy status before update is STARTING.
    assert proxy_state.status == ProxyStatus.STARTING

    # First failure shouldn't trigger state-transition to UNHEALTHY
    proxy_state.reconcile()
    assert proxy_state.status == ProxyStatus.STARTING

    # After PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD failures, state should
    # transition to UNHEALTHY
    for _ in range(PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD - 1):
        proxy_state.reconcile()

    assert proxy_state.status == ProxyStatus.UNHEALTHY


@patch("ray.serve._private.proxy_state.PROXY_READY_CHECK_TIMEOUT_S", 5)
def test_proxy_state_reconcile_health_check_succeed():
    timer = MockTimer()
    proxy_state = _create_proxy_state(time=timer)

    # Emulate readiness check succeeding
    proxy_state._actor_proxy_wrapper.is_ready_response = True
    # Should transition to HEALTHY
    proxy_state.reconcile()
    assert proxy_state.status == ProxyStatus.HEALTHY

    # Trigger update few more times and the status continue to be HEALTHY.
    for _ in range(10):
        _reconcile_and_check_proxy_status(proxy_state, ProxyStatus.HEALTHY)
        # Advance timer by 5s
        timer.advance(5)

    # Health-checks should have been performed on every iteration
    assert proxy_state._actor_proxy_wrapper.num_health_checks == 10


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 5)
def test_proxy_state_reconcile_health_check_transient_failures():
    timer = MockTimer()
    # Start with HEALTHY state
    proxy_state = _create_proxy_state(status=ProxyStatus.HEALTHY, timer=timer)
    # Simulate health-checks failing
    proxy_state._actor_proxy_wrapper.is_healthy_response = False

    # Reconcile PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD - 1 times, state should
    # continue to stay HEALTHY
    for _ in range(PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD - 1):
        _reconcile_and_check_proxy_status(proxy_state, ProxyStatus.HEALTHY)
        # Advance timer by 5 (to trigger new health-check)
        timer.advance(5)

    assert (
        proxy_state._actor_proxy_wrapper.get_num_health_checks()
        == PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD - 1
    )

    # Simulate health-checks passing
    proxy_state._actor_proxy_wrapper.is_healthy_response = True

    wait_for_condition(
        condition_predictor=_reconcile_and_check_proxy_status,
        state=proxy_state,
        status=ProxyStatus.HEALTHY,
    )
    # Ensure _consecutive_health_check_failures is reset
    assert proxy_state._consecutive_health_check_failures == 0


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 5)
def test_proxy_state_reconcile_health_check_persistent_failures():
    timer = MockTimer()
    # Start with HEALTHY state
    proxy_state = _create_proxy_state(status=ProxyStatus.HEALTHY, timer=timer)
    # Simulate health-checks failing
    proxy_state._actor_proxy_wrapper.is_healthy_response = False

    # Reconcile PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD - 1 times, state should
    # continue to stay HEALTHY
    for _ in range(PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD - 1):
        _reconcile_and_check_proxy_status(proxy_state, ProxyStatus.HEALTHY)
        # Advance timer by 5 (to trigger new health-check)
        timer.advance(5)

    assert (
        proxy_state._actor_proxy_wrapper.get_num_health_checks()
        == PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD - 1
    )

    # On PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD iteration, state transitions to
    # UNHEALTHY
    _reconcile_and_check_proxy_status(proxy_state, ProxyStatus.UNHEALTHY)
    # Ensure _consecutive_health_check_failures is correct
    assert proxy_state._consecutive_health_check_failures == 3


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 0)
@pytest.mark.parametrize("number_of_worker_nodes", [0, 1, 2, 3])
def test_proxy_manager_update_proxies_states(all_nodes, number_of_worker_nodes):
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


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_PERIOD_S", 5)
def test_proxy_state_reconcile_draining_success():
    """Test that the proxy will remain DRAINING even if health check succeeds."""
    timer = MockTimer(start_time=0)
    # Start with HEALTHY state
    proxy_state = _create_proxy_state(status=ProxyStatus.HEALTHY, timer=timer)
    # Simulate health-checks passing
    proxy_state._actor_proxy_wrapper.is_healthy_response = True
    # Simulate is_drained returning false
    proxy_state._actor_proxy_wrapper.is_drained_response = False

    for _ in range(10):
        proxy_state.reconcile(draining=True)
        assert proxy_state.status == ProxyStatus.DRAINING
        # Advance timer by 5 (to trigger new health-check, drain-check)
        timer.advance(5)

    assert proxy_state._actor_proxy_wrapper.get_num_health_checks() == 10
    assert proxy_state._actor_proxy_wrapper.get_num_drain_checks() == 10

    # Make sure the status is still DRAINING
    assert proxy_state.status == ProxyStatus.DRAINING

    # Simulate is_drained request to ProxyActor pending (for 5 iterations)
    proxy_state._actor_proxy_wrapper.is_drained_response = None

    for _ in range(5):
        proxy_state.reconcile(draining=True)
        assert proxy_state.status == ProxyStatus.DRAINING
        # Advance timer by 5 (to trigger new health-check, drain-check)
        timer.advance(5)

    assert proxy_state._actor_proxy_wrapper.get_num_health_checks() == 15
    # No new drain checks will occur, since there's a pending one (not completed yet)
    assert proxy_state._actor_proxy_wrapper.get_num_drain_checks() == 15

    # Simulate draining completed
    proxy_state._actor_proxy_wrapper.is_drained_response = True
    # Advance timer by 5 (to trigger new health-check, drain-check on next iteration)
    timer.advance(5)

    proxy_state.reconcile(draining=True)
    # State should transition to DRAINED
    assert proxy_state.status == ProxyStatus.DRAINED


@patch("ray.serve._private.proxy_state.PROXY_DRAIN_CHECK_PERIOD_S", 5)
@pytest.mark.parametrize("number_of_worker_nodes", [1])
def test_proxy_actor_manager_removing_proxies(all_nodes, number_of_worker_nodes):
    """Test the state transition from DRAINING to UNHEALTHY for the proxy actor."""

    assert len(all_nodes) == 2, "There should be 2 nodes in this test"

    timer = MockTimer(start_time=0)

    manager, cluster_node_info_cache = _create_proxy_state_manager(
        HTTPOptions(location=DeploymentMode.EveryNode),
        timer=timer,
    )
    cluster_node_info_cache.alive_nodes = all_nodes

    for node_id, _ in all_nodes:
        manager._proxy_states[node_id] = _create_proxy_state(
            status=ProxyStatus.STARTING,
            node_id=node_id,
            timer=timer,
        )

        manager._proxy_states[node_id]._actor_proxy_wrapper.is_ready_response = True

    # All nodes are target proxy nodes
    node_ids = [node_id for node_id, _ in all_nodes]

    worker_node_id = node_ids[1]
    worker_proxy_state = manager._proxy_states[worker_node_id]

    # Reconcile all proxies states
    manager.update(
        proxy_nodes=(set(node_ids)),
    )

    # Assert all proxies are HEALTHY
    proxy_statuses = [manager._proxy_states[node_id].status for node_id in node_ids]
    assert [ProxyStatus.HEALTHY, ProxyStatus.HEALTHY] == proxy_statuses

    # Reconcile proxies with empty set of target nodes (ie only proxy on the head-node
    # should be preserved all the other should be drained)
    worker_proxy_state._actor_proxy_wrapper.is_drained_response = False
    N = 10
    for _ in range(N):
        manager.update(
            proxy_nodes=set(),
        )
        timer.advance(5)
        # Assert that
        #   - Head-node proxy is HEALTHY4
        #   - Worker node proxy is DRAINING
        proxy_statuses = [manager._proxy_states[node_id].status for node_id in node_ids]
        assert [ProxyStatus.HEALTHY, ProxyStatus.DRAINING] == proxy_statuses

    assert worker_proxy_state._actor_proxy_wrapper.get_num_drain_checks() == N

    # Mark target proxy as fully drained
    worker_proxy_state._actor_proxy_wrapper.is_drained_response = True

    # Reconcile proxies with empty set of target nodes (worker node proxy
    # will be shutdown by now)
    manager.update(
        proxy_nodes=set(),
    )

    assert len(manager._proxy_states) == 1
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


@patch("ray.serve._private.proxy_state.PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD", 1)
@pytest.mark.parametrize("number_of_worker_nodes", [1])
def test_proxy_state_manager_timing_out_on_start(number_of_worker_nodes, all_nodes):
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

    # Ensure 2 proxies are created, one for the head node and another for the worker.
    assert len(proxy_state_manager._proxy_states) == len(node_ids)

    # Ensure the proxy state statuses before update are STARTING, set the
    # readiness check to failing
    for node_id in node_ids:
        proxy_state = proxy_state_manager._proxy_states[node_id]

        assert proxy_state.status == ProxyStatus.STARTING
        proxy_state._actor_proxy_wrapper.is_ready_response = False

    # Capture current proxy states (prior to updating)
    prev_proxy_states = {
        node_id: state for node_id, state in proxy_state_manager._proxy_states.items()
    }

    # Trigger PSM to reconcile
    proxy_state_manager.update(proxy_nodes=node_ids)

    # Ensure the proxy state statuses before update are STARTING, set the
    # readiness check to failing
    for node_id in node_ids:
        proxy_state = proxy_state_manager._proxy_states[node_id]
        prev_proxy_state = prev_proxy_states[node_id]
        # Assert
        #   - All proxies are restarted
        #   - Previous proxy states are UNHEALTHY
        #   - New proxy states are STARTING
        assert proxy_state != prev_proxy_state
        assert prev_proxy_state.status == ProxyStatus.UNHEALTHY
        assert proxy_state.status == ProxyStatus.STARTING
        # Mark new proxy readiness checks as passing
        proxy_state._actor_proxy_wrapper.is_ready_response = True

    # Capture current proxy states again (prior to updating)
    prev_proxy_states = {
        node_id: state for node_id, state in proxy_state_manager._proxy_states.items()
    }

    # Trigger PSM to reconcile
    proxy_state_manager.update(proxy_nodes=node_ids)

    # Ensure the proxy state statuses before update are STARTING, set the
    # readiness check to failing
    for node_id in node_ids:
        proxy_state = proxy_state_manager._proxy_states[node_id]
        prev_proxy_state = prev_proxy_states[node_id]
        # Assert
        #   - All proxies are restarted
        #   - Previous proxy states are UNHEALTHY
        #   - New proxy states are STARTING
        assert proxy_state == prev_proxy_state
        assert prev_proxy_state.status == ProxyStatus.HEALTHY
        assert proxy_state.status == ProxyStatus.HEALTHY


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
