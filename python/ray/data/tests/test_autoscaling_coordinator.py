from unittest.mock import Mock, patch

import pytest

import ray
from ray.cluster_utils import Cluster
from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
    HEAD_NODE_RESOURCE_LABEL,
    DefaultAutoscalingCoordinator,
    _AutoscalingCoordinatorActor,
    get_or_create_autoscaling_coordinator,
)
from ray.tests.conftest import wait_for_condition


def kill_autoscaling_coordinator():
    """Kill the AutoscalingCoordinator actor.

    We expose this to keep autoscaling coordinator tests isolated.

    If the AutoscalingCoordinator actor doesn't exist, this function is a no-op.
    """
    try:
        actor = ray.get_actor(
            "AutoscalingCoordinator", namespace="AutoscalingCoordinator"
        )
    except ValueError:
        # If the actor doesn't exist, `ray.get_actor` raises a `ValueError`.
        return

    ray.kill(actor)


@pytest.fixture
def teardown_autoscaling_coordinator():
    yield
    kill_autoscaling_coordinator()


CLUSTER_NODES_WITH_HEAD = [
    # Head node should be included if it has non-zero CPUs or GPUs.
    {
        "Resources": {
            "CPU": 10,
            "GPU": 5,
            "object_store_memory": 1000,
            HEAD_NODE_RESOURCE_LABEL: 1,
        },
        "Alive": True,
    },
    # Dead node should be excluded.
    {
        "Resources": {
            "CPU": 10,
            "GPU": 5,
            "object_store_memory": 1000,
        },
        "Alive": False,
    },
]

CLUSTER_NODES_WITHOUT_HEAD = [
    {
        "Resources": {"CPU": 10, "GPU": 5, "object_store_memory": 1000},
        "Alive": True,
    },
    # Head node should be excluded if CPUs and GPUs are both 0.
    {
        "Resources": {
            "CPU": 0,
            "GPU": 0,
            "object_store_memory": 1000,
            HEAD_NODE_RESOURCE_LABEL: 1,
        },
        "Alive": True,
    },
]


@pytest.mark.parametrize(
    "cluster_nodes",
    [
        CLUSTER_NODES_WITH_HEAD,
        CLUSTER_NODES_WITHOUT_HEAD,
    ],
)
def test_basic(cluster_nodes):
    mocked_time = 0

    mock_request_resources = Mock()
    as_coordinator = _AutoscalingCoordinatorActor(
        get_current_time=lambda: mocked_time,
        send_resources_request=mock_request_resources,
        get_cluster_nodes=lambda: cluster_nodes,
    )

    req1 = [{"CPU": 3, "GPU": 1, "object_store_memory": 100}]
    req1_timeout = 2
    as_coordinator.request_resources(
        requester_id="requester1",
        resources=req1,
        expire_after_s=req1_timeout,
    )
    mock_request_resources.assert_called_once_with(req1)
    res1 = as_coordinator.get_allocated_resources("requester1")

    def _remove_head_node_resources(res):
        for r in res:
            if HEAD_NODE_RESOURCE_LABEL in r:
                del r[HEAD_NODE_RESOURCE_LABEL]

    _remove_head_node_resources(res1)
    assert res1 == req1

    # Send the same request again. `mock_request_resources` won't be called
    # since the request is not updated.
    as_coordinator.request_resources(
        requester_id="requester1",
        resources=req1,
        expire_after_s=req1_timeout,
    )
    assert mock_request_resources.call_count == 1

    # Send a request from requester2, with request_remaining=True.
    # requester2 should get the requested + the remaining resources.
    req2 = [{"CPU": 2, "GPU": 1, "object_store_memory": 100}]
    req2_timeout = 20
    as_coordinator.request_resources(
        requester_id="requester2",
        resources=req2,
        expire_after_s=req2_timeout,
        request_remaining=True,
    )
    mock_request_resources.assert_called_with(req1 + req2)
    res2 = as_coordinator.get_allocated_resources("requester2")
    _remove_head_node_resources(res2)
    assert res2 == req2 + [{"CPU": 5, "GPU": 3, "object_store_memory": 800}]

    # Test updating req1
    req1_updated = [{"CPU": 4, "GPU": 2, "object_store_memory": 300}]
    as_coordinator.request_resources(
        requester_id="requester1",
        resources=req1_updated,
        expire_after_s=req1_timeout,
    )
    mock_request_resources.assert_called_with(req1_updated + req2)
    res1 = as_coordinator.get_allocated_resources("requester1")
    _remove_head_node_resources(res1)
    assert res1 == req1_updated
    res2 = as_coordinator.get_allocated_resources("requester2")
    _remove_head_node_resources(res2)
    assert res2 == req2 + [{"CPU": 4, "GPU": 2, "object_store_memory": 600}]

    # After req1_timeout, req1 should be expired.
    mocked_time = req1_timeout + 0.1
    as_coordinator._tick()
    mock_request_resources.assert_called_with(req2)
    res1 = as_coordinator.get_allocated_resources("requester1")
    res2 = as_coordinator.get_allocated_resources("requester2")
    _remove_head_node_resources(res1)
    _remove_head_node_resources(res2)
    assert res1 == []
    assert res2 == req2 + [{"CPU": 8, "GPU": 4, "object_store_memory": 900}]

    # After req2_timeout, req2 should be expired.
    mocked_time = req2_timeout + 0.1
    as_coordinator._tick()
    mock_request_resources.assert_called_with([])
    res1 = as_coordinator.get_allocated_resources("requester1")
    res2 = as_coordinator.get_allocated_resources("requester2")
    _remove_head_node_resources(res1)
    _remove_head_node_resources(res2)
    assert res1 == []
    assert res2 == []

    # Test canceling a request
    as_coordinator.cancel_request("requester2")
    res2 = as_coordinator.get_allocated_resources("requester2")
    _remove_head_node_resources(res2)
    assert res2 == []


def test_double_allocation_with_multiple_request_remaining():
    """Test fair allocation when multiple requesters have request_remaining=True."""
    cluster_nodes = [
        {
            "Resources": {
                "CPU": 10,
                "GPU": 5,
                "object_store_memory": 1000,
            },
            "Alive": True,
        }
    ]

    mocked_time = 0
    mock_request_resources = Mock()
    coordinator = _AutoscalingCoordinatorActor(
        get_current_time=lambda: mocked_time,
        send_resources_request=mock_request_resources,
        get_cluster_nodes=lambda: cluster_nodes,
    )

    # Requester1: asks for CPU=2, GPU=1 with request_remaining=True
    req1 = [{"CPU": 2, "GPU": 1, "object_store_memory": 100}]
    coordinator.request_resources(
        requester_id="requester1",
        resources=req1,
        expire_after_s=100,
        request_remaining=True,
    )

    # Requester2: asks for CPU=3, GPU=1 with request_remaining=True
    req2 = [{"CPU": 3, "GPU": 1, "object_store_memory": 200}]
    coordinator.request_resources(
        requester_id="requester2",
        resources=req2,
        expire_after_s=100,
        request_remaining=True,
    )

    # Get allocated resources
    res1 = coordinator.get_allocated_resources("requester1")
    res2 = coordinator.get_allocated_resources("requester2")

    # After allocating specific requests (req1 and req2):
    # Remaining = CPU: 10-2-3=5, GPU: 5-1-1=3, memory: 1000-100-200=700
    # With fair allocation, each requester gets 1/2 of remaining resources
    expected_remaining_per_requester = {
        "CPU": 5 // 2,  # = 2
        "GPU": 3 // 2,  # = 1
        "object_store_memory": 700 // 2,  # = 350
    }

    # Both requesters should get their specific requests + fair share of remaining
    assert res1 == req1 + [expected_remaining_per_requester]
    assert res2 == req2 + [expected_remaining_per_requester]


@pytest.fixture
def cluster():
    """Initialize a Ray cluster with a 0 CPU head node and no workers."""
    cluster = Cluster()
    cluster.add_node(num_cpus=0)
    cluster.wait_for_nodes()
    cluster.connect()
    yield cluster
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.parametrize("gpu_tasks_include_cpu", [True, False])
def test_autoscaling_coordinator_e2e(cluster, gpu_tasks_include_cpu):
    """Integration test for AutoscalingCoordinator.

    This test creates 2 dummy components that request resources from
    AutoscalingCoordinator, and checks allocated resources are correct.
    """
    object_store_memory = 100 * 1024**2
    num_cpu_nodes = 4
    cpu_node_spec = {"num_cpus": 8, "object_store_memory": object_store_memory}
    num_gpu_nodes = 2
    gpu_node_spec = {
        "num_cpus": 4,
        "num_gpus": 1,
        "object_store_memory": object_store_memory,
    }

    for _ in range(num_cpu_nodes):
        cluster.add_node(**cpu_node_spec)
    for _ in range(num_gpu_nodes):
        cluster.add_node(**gpu_node_spec)

    cluster.wait_for_nodes()

    @ray.remote
    def request_and_check_resources(
        requester_id, resources, expected, request_remaining
    ):
        as_coordinator = get_or_create_autoscaling_coordinator()
        ray.get(
            as_coordinator.request_resources.remote(
                requester_id=requester_id,
                resources=resources,
                expire_after_s=100,
                request_remaining=request_remaining,
            )
        )

        def check_allocated_resources():
            allocated = ray.get(
                as_coordinator.get_allocated_resources.remote(requester_id)
            )
            allocated = [
                {
                    k: int(v)
                    for k, v in r.items()
                    if k in ["CPU", "GPU", "object_store_memory"] and v != 0
                }
                for r in allocated
                if "node:__internal_head__" not in r
            ]
            allocated = [r for r in allocated if len(r) > 0]
            if allocated != expected:
                print(
                    f"{requester_id}: Allocated resources: {allocated}, "
                    f"expected: {expected}. Retrying."
                )
                return False
            else:
                return True

        wait_for_condition(
            check_allocated_resources,
            retry_interval_ms=1000,
            timeout=5,
        )
        return "ok"

    res1_resources = [
        {
            "CPU": cpu_node_spec["num_cpus"],
            "object_store_memory": object_store_memory,
        }
    ] * num_cpu_nodes
    req2_resources = [
        {
            "GPU": gpu_node_spec["num_gpus"],
        }
    ] * num_gpu_nodes
    if gpu_tasks_include_cpu:
        for r in req2_resources:
            r["CPU"] = 1
    remaining = [
        {
            "CPU": gpu_node_spec["num_cpus"] - (1 if gpu_tasks_include_cpu else 0),
            "object_store_memory": object_store_memory,
        }
    ] * num_gpu_nodes

    res1 = request_and_check_resources.remote(
        requester_id="requester1",
        resources=res1_resources,
        expected=res1_resources + remaining,
        request_remaining=True,
    )
    res2 = request_and_check_resources.remote(
        requester_id="requester2",
        resources=req2_resources,
        expected=req2_resources,
        request_remaining=False,
    )

    assert ray.get([res1, res2]) == ["ok"] * 2


def _test_consecutive_failures(
    coordinator,
    call_method,
    counter_attr,
    error_msg_prefix,
):
    """Test consecutive failures: increment counter, raise after max, reset on success."""
    max_failures = coordinator.MAX_CONSECUTIVE_FAILURES
    timeout_error = ray.exceptions.GetTimeoutError("timeout")

    # Counter increments on each failure
    with patch("ray.get", side_effect=timeout_error):
        for attempt in range(1, max_failures):
            call_method()
            assert getattr(coordinator, counter_attr) == attempt

    # Exception raised after max consecutive failures
    expected_error_msg = f"{error_msg_prefix} after {max_failures} consecutive failures"
    with patch("ray.get", side_effect=timeout_error):
        with pytest.raises(RuntimeError, match=expected_error_msg):
            call_method()

    # Counter resets on success
    with patch("ray.get", return_value=None):
        call_method()
        assert getattr(coordinator, counter_attr) == 0


def test_get_allocated_resources_handles_timeout_error(
    teardown_autoscaling_coordinator,
):
    """Test get_allocated_resources handles timeout error."""
    coordinator = DefaultAutoscalingCoordinator()
    coordinator._cached_allocated_resources["test"] = [{"CPU": 1}]

    def call_method():
        return coordinator.get_allocated_resources("test")

    max_failures = coordinator.MAX_CONSECUTIVE_FAILURES
    timeout_error = ray.exceptions.GetTimeoutError("timeout")
    cached_value = [{"CPU": 1}]
    new_value = [{"CPU": 2}]

    # Counter increments on failures and returns cached value
    with patch("ray.get", side_effect=timeout_error):
        for attempt in range(1, max_failures):
            result = call_method()
            assert result == cached_value
            assert coordinator._consecutive_failures_get_allocated_resources == attempt

    # Exception raised after max consecutive failures
    expected_error_msg = (
        f"Failed to get allocated resources for test "
        f"after {max_failures} consecutive failures"
    )
    with patch("ray.get", side_effect=timeout_error):
        with pytest.raises(RuntimeError, match=expected_error_msg):
            call_method()

    # Counter resets on success and returns new value
    with patch("ray.get", return_value=new_value):
        result = call_method()
        assert result == new_value
        assert coordinator._consecutive_failures_get_allocated_resources == 0


def test_cancel_request_handles_timeout_error(teardown_autoscaling_coordinator):
    """Test cancel_request handles timeout error."""
    coordinator = DefaultAutoscalingCoordinator()

    _test_consecutive_failures(
        coordinator=coordinator,
        call_method=lambda: coordinator.cancel_request("test"),
        counter_attr="_consecutive_failures_cancel_request",
        error_msg_prefix="Failed to cancel resource request for test",
    )


def test_request_resources_handles_timeout_error(teardown_autoscaling_coordinator):
    """Test request_resources handles timeout error."""
    coordinator = DefaultAutoscalingCoordinator()

    _test_consecutive_failures(
        coordinator=coordinator,
        call_method=lambda: coordinator.request_resources(
            "test", [{"CPU": 1}], expire_after_s=1
        ),
        counter_attr="_consecutive_failures_request_resources",
        error_msg_prefix="Failed to send resource request for test",
    )


def test_coordinator_accepts_zero_resource_for_missing_resource_type(
    teardown_autoscaling_coordinator,
):
    # This is a regression test for a bug where the coordinator crashes when you request
    # a resource type (e.g., GPU: 0) that doesn't exist on the cluster.
    coordinator = DefaultAutoscalingCoordinator()

    coordinator.request_resources(
        requester_id="spam", resources=[{"CPU": 1, "GPU": 0}], expire_after_s=1
    )

    assert coordinator.get_allocated_resources("spam") == [{"CPU": 1, "GPU": 0}]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
