import copy
from unittest.mock import Mock

import pytest

import ray
from ray.cluster_utils import Cluster
from ray.data._internal.cluster_autoscaler.base_autoscaling_coordinator import (
    STANDARD_RESOURCE_TYPES,
    ResourceRequestStrategy,
)
from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
    HEAD_NODE_RESOURCE_LABEL,
    DefaultAutoscalingCoordinator,
    _AutoscalingCoordinatorActor,
    _format_node_resources_for_log,
    get_or_create_autoscaling_coordinator,
)
from ray.data._internal.util import GiB
from ray.tests.conftest import wait_for_condition

CLUSTER_NODES_WITH_HEAD = [
    # Head node should be included if it has non-zero CPUs or GPUs.
    {
        "NodeID": "n1",
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
        "NodeID": "n-dead",
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
        "NodeID": "n1",
        "Resources": {"CPU": 10, "GPU": 5, "object_store_memory": 1000},
        "Alive": True,
    },
    # Head node should be excluded if CPUs and GPUs are both 0.
    {
        "NodeID": "n-head-zero",
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
    res1 = as_coordinator.get_reserved_resources("requester1")
    assert res1 == {"n1": {"CPU": 3, "GPU": 1, "object_store_memory": 100}}

    # Send the same request again. `mock_request_resources` won't be called
    # since the request is not updated.
    as_coordinator.request_resources(
        requester_id="requester1",
        resources=req1,
        expire_after_s=req1_timeout,
    )
    assert mock_request_resources.call_count == 1

    # Send a request from requester2, with request_remaining=STANDARD_RESOURCE_TYPES.
    # requester2 should get the requested + the remaining resources (merged per node).
    req2 = [{"CPU": 2, "GPU": 1, "object_store_memory": 100}]
    req2_timeout = 20
    as_coordinator.request_resources(
        requester_id="requester2",
        resources=req2,
        expire_after_s=req2_timeout,
        request_remaining=STANDARD_RESOURCE_TYPES,
    )
    mock_request_resources.assert_called_with(req1 + req2)
    res2 = as_coordinator.get_reserved_resources("requester2")
    # req2 explicit (2/1/100) + remaining (5/3/800) merged on same node.
    assert res2 == {"n1": {"CPU": 7, "GPU": 4, "object_store_memory": 900}}

    # Test updating req1
    req1_updated = [{"CPU": 4, "GPU": 2, "object_store_memory": 300}]
    as_coordinator.request_resources(
        requester_id="requester1",
        resources=req1_updated,
        expire_after_s=req1_timeout,
    )
    mock_request_resources.assert_called_with(req1_updated + req2)
    res1 = as_coordinator.get_reserved_resources("requester1")
    assert res1 == {"n1": {"CPU": 4, "GPU": 2, "object_store_memory": 300}}
    res2 = as_coordinator.get_reserved_resources("requester2")
    # req2 explicit (2/1/100) + remaining after req1_updated (4/2/600) merged.
    assert res2 == {"n1": {"CPU": 6, "GPU": 3, "object_store_memory": 700}}

    # After req1_timeout, req1 should be expired.
    mocked_time = req1_timeout + 0.1
    as_coordinator._tick()
    mock_request_resources.assert_called_with(req2)
    res1 = as_coordinator.get_reserved_resources("requester1")
    res2 = as_coordinator.get_reserved_resources("requester2")
    assert res1 == {}
    # req2 explicit (2/1/100) + full remaining (8/4/900) merged.
    assert res2 == {"n1": {"CPU": 10, "GPU": 5, "object_store_memory": 1000}}

    # After req2_timeout, req2 should be expired.
    mocked_time = req2_timeout + 0.1
    as_coordinator._tick()
    mock_request_resources.assert_called_with([])
    res1 = as_coordinator.get_reserved_resources("requester1")
    res2 = as_coordinator.get_reserved_resources("requester2")
    assert res1 == {}
    assert res2 == {}

    # Test canceling a request
    as_coordinator.cancel_request("requester2")
    res2 = as_coordinator.get_reserved_resources("requester2")
    assert res2 == {}


def test_double_allocation_with_multiple_request_remaining():
    """Test fair allocation when multiple requesters have request_remaining=STANDARD_RESOURCE_TYPES."""
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

    # Requester1: asks for CPU=2, GPU=1 with request_remaining=STANDARD_RESOURCE_TYPES
    req1 = [{"CPU": 2, "GPU": 1, "object_store_memory": 100}]
    coordinator.request_resources(
        requester_id="requester1",
        resources=req1,
        expire_after_s=100,
        request_remaining=STANDARD_RESOURCE_TYPES,
    )

    # Requester2: asks for CPU=3, GPU=1 with request_remaining=STANDARD_RESOURCE_TYPES
    req2 = [{"CPU": 3, "GPU": 1, "object_store_memory": 200}]
    coordinator.request_resources(
        requester_id="requester2",
        resources=req2,
        expire_after_s=100,
        request_remaining=STANDARD_RESOURCE_TYPES,
    )

    # Get reserved resources (per-node dict, explicit + remaining merged per node)
    res1 = coordinator.get_reserved_resources("requester1")
    res2 = coordinator.get_reserved_resources("requester2")

    # After allocating specific requests (req1 and req2):
    # Remaining = CPU: 10-2-3=5, GPU: 5-1-1=3, memory: 1000-100-200=700
    # With fair allocation, each requester gets 1/2 of remaining resources.
    # Explicit + remaining are merged on the same (single) node keyed by "n1".
    assert res1 == {"": {"CPU": 2 + 2, "GPU": 1 + 1, "object_store_memory": 100 + 350}}
    assert res2 == {"": {"CPU": 3 + 2, "GPU": 1 + 1, "object_store_memory": 200 + 350}}


def test_request_remaining_records_leftover_types_not_in_bundles():
    """Leftovers are reserved for types in ``request_remaining``, not bundle keys."""
    cluster_nodes = [
        {
            "Resources": {"CPU": 10, "object_store_memory": 1000, "GPU": 2},
            "Alive": True,
        }
    ]
    coordinator = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=Mock(),
        get_cluster_nodes=lambda: cluster_nodes,
    )

    coordinator.request_resources(
        requester_id="requester1",
        resources=[{"CPU": 2}],
        expire_after_s=100,
        # Ask for CPU + object_store leftovers, but not GPU.
        request_remaining=["CPU", "object_store_memory"],
    )

    res = coordinator.get_reserved_resources("requester1")
    assert res == {"": {"CPU": 10, "object_store_memory": 1000}}


def test_request_remaining_empty_means_no_leftovers():
    """An empty ``request_remaining`` set does not reserve leftover capacity."""
    cluster_nodes = [
        {
            "Resources": {"CPU": 10, "object_store_memory": 1000},
            "Alive": True,
        }
    ]
    coordinator = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=Mock(),
        get_cluster_nodes=lambda: cluster_nodes,
    )

    coordinator.request_resources(
        requester_id="requester1",
        resources=[{"CPU": 2}],
        expire_after_s=100,
        request_remaining=[],
    )

    res = coordinator.get_reserved_resources("requester1")
    assert res == {"": {"CPU": 2}}


def test_format_node_resources_for_log():
    # Two nodes with standard resources (plus custom labels and GPU: 0) and
    # one node with only custom/zero resources. Custom/zero resources are
    # dropped; the empty node is omitted; each remaining node is listed.
    resources = {
        "n1": {
            "CPU": 8,
            "GPU": 0,
            "memory": 32 * GiB,
            "object_store_memory": int(8.96 * GiB),
            "anyscale/cpu_only:true": 1.0,
            "anyscale/region:us-west-2": 1.0,
            "node:10.0.193.159": 1.0,
        },
        "n2": {
            "CPU": 8,
            "GPU": 0,
            "memory": 32 * GiB,
            "object_store_memory": int(9.04 * GiB),
            "anyscale/cpu_only:true": 1.0,
            "anyscale/region:us-west-2": 1.0,
            "node:10.0.241.173": 1.0,
        },
        "n3": {
            "CPU": 0,
            "GPU": 0,
            "memory": 0,
            "object_store_memory": 0,
            "anyscale/cpu_only:true": 1.0,
            "node:10.0.252.14": 1.0,
        },
    }

    log_message = _format_node_resources_for_log(resources)
    assert log_message == (
        "[n1: {CPU: 8, memory: 32.0GiB, object_store_memory: 9.0GiB}, "
        "n2: {CPU: 8, memory: 32.0GiB, object_store_memory: 9.0GiB}]"
    )
    assert "anyscale/" not in log_message
    assert "node:" not in log_message
    assert "GPU" not in log_message
    assert "n3" not in log_message


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
            reserved = ray.get(
                as_coordinator.get_reserved_resources.remote(requester_id)
            )
            # Convert per-node dict to a sorted list of resource dicts for comparison.
            # Only keep keys present in expected; real nodes also carry "memory"
            # (physical RAM) which doesn't appear in the expected bundles.
            _KEEP = {"CPU", "GPU", "object_store_memory"}
            _sort_key = lambda r: sorted(r.items())  # noqa: E731
            allocated = sorted(
                [
                    r
                    for node_res in reserved.values()
                    for r in [
                        {
                            k: int(v)
                            for k, v in node_res.items()
                            if k in _KEEP and v != 0
                        }
                    ]
                    if r
                ],
                key=_sort_key,
            )
            expected_sorted = sorted(expected, key=_sort_key)
            if allocated != expected_sorted:
                print(
                    f"{requester_id}: Allocated resources: {allocated}, "
                    f"expected: {expected_sorted}. Retrying."
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
        request_remaining=STANDARD_RESOURCE_TYPES,
    )
    res2 = request_and_check_resources.remote(
        requester_id="requester2",
        resources=req2_resources,
        expected=req2_resources,
        request_remaining=None,
    )

    assert ray.get([res1, res2]) == ["ok"] * 2


@pytest.fixture
def autoscaling_coordinator_actor(ray_start_regular_shared):
    actor_cls = ray.remote(num_cpus=0)(_AutoscalingCoordinatorActor)
    actor = actor_cls.remote(
        send_resources_request=lambda b: None,
        get_cluster_nodes=lambda: [
            {"Alive": True, "Resources": {"CPU": 4}, "NodeID": "n1"}
        ],
    )
    yield actor
    ray.kill(actor)


def test_get_reserved_resources_eventually_consistent(autoscaling_coordinator_actor):
    """get_reserved_resources eventually reflects a submitted request_resources call."""
    coordinator = DefaultAutoscalingCoordinator(
        "test", autoscaling_coordinator_actor=autoscaling_coordinator_actor
    )

    coordinator.request_resources(resources=[{"CPU": 1}], expire_after_s=60)

    wait_for_condition(
        lambda: coordinator.get_reserved_resources() == {"n1": {"CPU": 1}},
        retry_interval_ms=100,
        timeout=5,
    )


def test_get_reserved_resources_returns_cached_while_pending(
    autoscaling_coordinator_actor, monkeypatch
):
    """Returns the last cached value without blocking when a ref is still in-flight."""
    coordinator = DefaultAutoscalingCoordinator(
        "test", autoscaling_coordinator_actor=autoscaling_coordinator_actor
    )

    coordinator.request_resources(resources=[{"CPU": 1}], expire_after_s=60)
    wait_for_condition(
        lambda: coordinator.get_reserved_resources() == {"n1": {"CPU": 1}},
        retry_interval_ms=100,
        timeout=5,
    )

    # Make ray.wait report all refs as still pending.
    def fake_wait(refs, *args, **kwargs):
        return [], refs

    monkeypatch.setattr(ray, "wait", fake_wait)

    coordinator.request_resources(resources=[{"CPU": 2}], expire_after_s=60)
    # Should return the stale cached value, not block.
    assert coordinator.get_reserved_resources() == {"n1": {"CPU": 1}}


def test_get_reserved_resources_returns_cached_on_actor_error(
    autoscaling_coordinator_actor, monkeypatch
):
    """Actor errors fall back to the cached value, log a warning, and never raise.

    Recovery is automatic: a fresh request is submitted on the next call.
    """
    coordinator = DefaultAutoscalingCoordinator(
        "test", autoscaling_coordinator_actor=autoscaling_coordinator_actor
    )

    coordinator.request_resources(resources=[{"CPU": 1}], expire_after_s=60)
    wait_for_condition(
        lambda: coordinator.get_reserved_resources() == {"n1": {"CPU": 1}},
        retry_interval_ms=100,
        timeout=5,
    )

    def fake_wait(refs, *args, **kwargs):
        # Report the ref as ready so ray.get is attempted.
        return refs, []

    monkeypatch.setattr(ray, "wait", fake_wait)
    monkeypatch.setattr(ray, "get", Mock(side_effect=ray.exceptions.RayActorError()))

    # Must return the last cached value, not raise.
    assert coordinator.get_reserved_resources() == {"n1": {"CPU": 1}}

    # Recovery: submit a new request after the error and verify it eventually
    # resolves, proving the coordinator can communicate with the actor again.
    monkeypatch.undo()
    coordinator.request_resources(resources=[{"CPU": 2}], expire_after_s=60)
    wait_for_condition(
        lambda: coordinator.get_reserved_resources() == {"n1": {"CPU": 2}},
        retry_interval_ms=100,
        timeout=5,
    )


def test_cancel_request_makes_get_return_empty(autoscaling_coordinator_actor):
    """After cancel_request, get_reserved_resources eventually returns {}."""
    coordinator = DefaultAutoscalingCoordinator(
        "test", autoscaling_coordinator_actor=autoscaling_coordinator_actor
    )

    coordinator.request_resources(resources=[{"CPU": 1}], expire_after_s=60)
    wait_for_condition(
        lambda: coordinator.get_reserved_resources() == {"n1": {"CPU": 1}},
        retry_interval_ms=100,
        timeout=5,
    )

    coordinator.cancel_request()
    wait_for_condition(
        lambda: coordinator.get_reserved_resources() == {},
        retry_interval_ms=100,
        timeout=5,
    )


def test_non_ray_errors_propagate(autoscaling_coordinator_actor, monkeypatch):
    """Non-Ray errors during result consumption propagate rather than being swallowed.

    Guards against accidentally broadening the catch from RayError to Exception.
    """
    coordinator = DefaultAutoscalingCoordinator(
        "test", autoscaling_coordinator_actor=autoscaling_coordinator_actor
    )

    coordinator.request_resources(resources=[{"CPU": 1}], expire_after_s=60)
    wait_for_condition(
        lambda: coordinator.get_reserved_resources() == {"n1": {"CPU": 1}},
        retry_interval_ms=100,
        timeout=5,
    )

    monkeypatch.setattr(ray, "wait", lambda refs, *a, **kw: (refs, []))
    monkeypatch.setattr(
        ray, "get", Mock(side_effect=ValueError("unexpected local error"))
    )

    with pytest.raises(ValueError, match="unexpected local error"):
        coordinator.get_reserved_resources()


def test_coordinator_accepts_zero_resource_for_missing_resource_type(
    autoscaling_coordinator_actor,
):
    # This is a regression test for a bug where the coordinator crashes when you request
    # a resource type (e.g., GPU: 0) that doesn't exist on the cluster.
    coordinator = DefaultAutoscalingCoordinator(
        "spam", autoscaling_coordinator_actor=autoscaling_coordinator_actor
    )

    coordinator.request_resources(resources=[{"CPU": 1, "GPU": 0}], expire_after_s=1)

    # GPU: 0 is a no-op; only CPU: 1 ends up reserved.
    wait_for_condition(
        lambda: coordinator.get_reserved_resources() == {"n1": {"CPU": 1}},
        retry_interval_ms=100,
        timeout=5,
    )


def test_fractional_bundles_are_forwarded_unchanged():
    """Fractional bundle values needs be forwarded to the autoscaler SDK as-is.

    Previously the coordinator rounded each value up to the next integer
    before forwarding (e.g. ``{"CPU": 0.1}`` became ``{"CPU": 1}``), which
    inflated the autoscaler's demand view by up to N× when training launched
    N workers with fractional ``resources_per_worker``."""
    mock_send = Mock()
    coord = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=mock_send,
        get_cluster_nodes=lambda: CLUSTER_NODES_WITHOUT_HEAD,
    )

    coord.request_resources(
        requester_id="r", resources=[{"CPU": 0.1}], expire_after_s=1
    )
    mock_send.assert_called_once_with([{"CPU": 0.1}])


def test_label_selectors_are_forwarded_to_sdk():
    """Per-bundle label_selectors are forwarded as ``bundle_label_selectors``."""
    mock_send = Mock()
    coord = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=mock_send,
        get_cluster_nodes=lambda: CLUSTER_NODES_WITHOUT_HEAD,
    )

    coord.request_resources(
        requester_id="r",
        resources=[{"CPU": 1}, {"CPU": 1}],
        label_selectors=[{"instance-type": "m6i.xlarge"}, {}],
        expire_after_s=10,
    )
    mock_send.assert_called_once_with(
        [{"CPU": 1}, {"CPU": 1}],
        label_selectors=[{"instance-type": "m6i.xlarge"}, {}],
    )


def test_sdk_forwarding_merges_subcluster_into_each_bundle():
    """Forwarded bundles union the per-bundle selector with the
    requester's subcluster."""
    mock_send = Mock()
    coord = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=mock_send,
        get_cluster_nodes=lambda: CLUSTER_NODES_WITHOUT_HEAD,
    )

    coord.request_resources(
        requester_id="r",
        resources=[{"CPU": 1}, {"CPU": 1}],
        label_selectors=[
            # Non-subcluster key preserved alongside the subcluster.
            {"node_id": "n1"},
            # Empty per-bundle entry — should still receive the subcluster.
            {},
        ],
        subcluster_selector={"ray-subcluster": "training"},
        expire_after_s=10,
    )
    mock_send.assert_called_once_with(
        [{"CPU": 1}, {"CPU": 1}],
        label_selectors=[
            {"node_id": "n1", "ray-subcluster": "training"},
            {"ray-subcluster": "training"},
        ],
    )


def test_label_selectors_length_mismatch_raises():
    coord = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=Mock(),
        get_cluster_nodes=lambda: CLUSTER_NODES_WITHOUT_HEAD,
    )
    with pytest.raises(ValueError, match="label_selectors length"):
        coord.request_resources(
            requester_id="r",
            resources=[{"CPU": 1}, {"CPU": 1}],
            label_selectors=[{"a": "b"}],
            expire_after_s=10,
        )


def test_request_rejects_per_bundle_cross_subcluster():
    """Per-bundle subcluster values that disagree with the requester's
    ``subcluster_selector`` raise."""
    coord = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=Mock(),
        get_cluster_nodes=lambda: CLUSTER_NODES_WITHOUT_HEAD,
    )
    with pytest.raises(ValueError, match="cross-subcluster"):
        coord.request_resources(
            requester_id="r",
            resources=[{"CPU": 1}, {"CPU": 1}],
            label_selectors=[
                {"ray-subcluster": "training"},
                {"ray-subcluster": "validation"},
            ],
            subcluster_selector={"ray-subcluster": "training"},
            expire_after_s=10,
        )


def test_request_rejects_changing_subcluster_selector():
    """A requester's ``subcluster_selector`` can't change between calls;
    the rejected call must also leave the registry untouched."""
    coord = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=Mock(),
        get_cluster_nodes=lambda: CLUSTER_NODES_WITHOUT_HEAD,
    )
    coord.request_resources(
        requester_id="r",
        resources=[{"CPU": 1}],
        subcluster_selector={"ray-subcluster": "training"},
        expire_after_s=10,
    )
    with pytest.raises(ValueError, match="Cannot change subcluster_selector"):
        coord.request_resources(
            requester_id="r",
            resources=[{"CPU": 1}],
            subcluster_selector={"ray-subcluster": "validation"},
            expire_after_s=10,
        )
    # Registry must be unchanged after the rejected call.
    assert coord._subcluster_selectors["r"] == {"ray-subcluster": "training"}


def test_label_selector_change_triggers_resend():
    """A request whose only change is the label selector should still be
    re-sent to the autoscaler."""
    mock_send = Mock()
    coord = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=mock_send,
        get_cluster_nodes=lambda: CLUSTER_NODES_WITHOUT_HEAD,
    )

    coord.request_resources(
        requester_id="r",
        resources=[{"CPU": 1}],
        label_selectors=[{"zone": "a"}],
        expire_after_s=10,
    )
    coord.request_resources(
        requester_id="r",
        resources=[{"CPU": 1}],
        label_selectors=[{"zone": "b"}],
        expire_after_s=10,
    )
    assert mock_send.call_count == 2
    mock_send.assert_called_with([{"CPU": 1}], label_selectors=[{"zone": "b"}])


LABELED_CLUSTER_NODES = [
    {
        "NodeID": "n-train-1",
        "Resources": {"CPU": 8, "object_store_memory": 1000},
        "Labels": {"ray-subcluster": "training"},
        "Alive": True,
    },
    {
        "NodeID": "n-train-2",
        "Resources": {"CPU": 8, "object_store_memory": 1000},
        "Labels": {"ray-subcluster": "training"},
        "Alive": True,
    },
    {
        "NodeID": "n-val-1",
        "Resources": {"CPU": 4, "object_store_memory": 500},
        "Labels": {"ray-subcluster": "validation"},
        "Alive": True,
    },
    {
        "NodeID": "n-default-1",
        "Resources": {"CPU": 2, "object_store_memory": 200},
        "Labels": {},
        "Alive": True,
    },
]


def _make_coordinator(nodes):
    return _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=Mock(),
        get_cluster_nodes=lambda: nodes,
    )


STRATEGY_CLUSTER_NODES = [
    {"NodeID": "n1", "Resources": {"GPU": 4}, "Alive": True},
    {"NodeID": "n2", "Resources": {"GPU": 4}, "Alive": True},
]


def _nodes(**node_id_to_resources):
    return [
        {"NodeID": node_id, "Resources": resources, "Alive": True}
        for node_id, resources in node_id_to_resources.items()
    ]


# ``strategy`` is omitted from the request entirely when it is ``None``, which
# is how the PACK-by-default case is expressed.
RESERVATION_LAYOUTS = [
    pytest.param(
        STRATEGY_CLUSTER_NODES,
        ResourceRequestStrategy.PACK,
        [{"GPU": 1}] * 4,
        {"n1": {"GPU": 4}},
        id="pack_fills_a_node_before_moving_on",
    ),
    pytest.param(
        STRATEGY_CLUSTER_NODES,
        None,
        [{"GPU": 1}] * 4,
        {"n1": {"GPU": 4}},
        id="pack_is_the_default",
    ),
    pytest.param(
        STRATEGY_CLUSTER_NODES,
        ResourceRequestStrategy.SPREAD,
        [{"GPU": 1}] * 4,
        {"n1": {"GPU": 2}, "n2": {"GPU": 2}},
        id="spread_round_robins",
    ),
    pytest.param(
        _nodes(n1={"GPU": 3}, n2={"GPU": 1}),
        ResourceRequestStrategy.SPREAD,
        [{"GPU": 1}] * 4,
        {"n1": {"GPU": 3}, "n2": {"GPU": 1}},
        # SPREAD is best effort: once n2 is full the rest fall back to n1
        # rather than going unreserved.
        id="spread_falls_back_to_a_node_with_room",
    ),
    pytest.param(
        STRATEGY_CLUSTER_NODES,
        ResourceRequestStrategy.STRICT_PACK,
        [{"GPU": 1}] * 4,
        {"n1": {"GPU": 4}},
        id="strict_pack_puts_every_bundle_on_one_node",
    ),
    pytest.param(
        _nodes(n1={"GPU": 3}, n2={"GPU": 3}),
        ResourceRequestStrategy.STRICT_PACK,
        [{"GPU": 1}] * 4,
        {},
        # All or nothing: splitting over two nodes is what STRICT_PACK forbids,
        # so a cluster with no node big enough reserves nothing at all.
        id="strict_pack_reserves_nothing_when_no_node_fits",
    ),
    pytest.param(
        STRATEGY_CLUSTER_NODES,
        ResourceRequestStrategy.STRICT_SPREAD,
        [{"GPU": 1}] * 4,
        {"n1": {"GPU": 1}, "n2": {"GPU": 1}},
        # Only 2 of the 4 bundles can be placed on a 2-node cluster.
        id="strict_spread_places_one_bundle_per_node",
    ),
    pytest.param(
        _nodes(n1={"GPU": 8}, n2={"GPU": 1}),
        ResourceRequestStrategy.STRICT_SPREAD,
        [{"GPU": 4}] * 2,
        {"n1": {"GPU": 4}},
        # n1 has room for the second bundle but is off limits, and n2 is too
        # small -- coming back short is what tells the caller to wait.
        id="strict_spread_refuses_to_stack_even_with_room",
    ),
    pytest.param(
        _nodes(n1={"CPU": 2, "GPU": 1}),
        ResourceRequestStrategy.PACK,
        [{"CPU": 8}, {"GPU": 1}],
        {"n1": {"GPU": 1}},
        id="pack_skips_a_bundle_that_fits_nowhere",
    ),
    pytest.param(
        _nodes(n1={"CPU": 2, "GPU": 1}),
        ResourceRequestStrategy.SPREAD,
        [{"CPU": 8}, {"GPU": 1}],
        {"n1": {"GPU": 1}},
        # The unplaceable bundle must not strand the one behind it, which asks
        # for a different resource type entirely.
        id="spread_skips_a_bundle_that_fits_nowhere",
    ),
    pytest.param(
        # Node ids run the opposite way round from capacity, so reserving on
        # the large node can only come from the capacity ordering.
        _nodes(**{"a-small": {"CPU": 4}, "z-large": {"CPU": 16}}),
        None,
        [{"CPU": 2}],
        {"z-large": {"CPU": 2}},
        id="largest_node_is_offered_first",
    ),
]


def _request_resources(coord, strategy, resources, requester_id="train"):
    kwargs = {} if strategy is None else {"strategy": strategy}
    coord.request_resources(
        requester_id=requester_id,
        resources=resources,
        expire_after_s=10,
        **kwargs,
    )


@pytest.mark.parametrize("nodes,strategy,resources,expected", RESERVATION_LAYOUTS)
def test_reservation_layout(nodes, strategy, resources, expected):
    """Which nodes a requester's bundles land on -- not just the total -- is
    determined by its strategy and by what the cluster can actually fit."""
    coord = _make_coordinator(nodes)
    _request_resources(coord, strategy, resources)

    assert coord.get_reserved_resources("train") == expected


@pytest.mark.parametrize("strategy", list(ResourceRequestStrategy))
def test_strategy_accepts_the_plain_string_form(strategy):
    """``ScalingConfig.placement_strategy`` is a plain ``str``, so the bare
    string has to behave exactly like the enum member."""
    resources = [{"GPU": 1}] * 4

    from_enum = _make_coordinator(STRATEGY_CLUSTER_NODES)
    _request_resources(from_enum, strategy, resources)

    from_string = _make_coordinator(STRATEGY_CLUSTER_NODES)
    _request_resources(from_string, strategy.value, resources)

    assert from_string.get_reserved_resources(
        "train"
    ) == from_enum.get_reserved_resources("train")


@pytest.mark.parametrize("strategy", list(ResourceRequestStrategy))
def test_resending_an_unchanged_request_is_a_noop(strategy):
    """Requesters refresh on a timer, so an unchanged re-send must leave the
    reservation alone and not look like an update worth re-forwarding."""
    coord = _make_coordinator(STRATEGY_CLUSTER_NODES)

    def send():
        # Fresh dicts each time: a requester re-sending its request is not
        # expected to hand back the same objects.
        _request_resources(coord, strategy, [{"GPU": 1} for _ in range(4)])

    send()
    reserved_after_first = copy.deepcopy(coord.get_reserved_resources("train"))
    sends_after_first = coord._send_resources_request.call_count

    send()
    send()

    assert coord.get_reserved_resources("train") == reserved_after_first
    assert coord._send_resources_request.call_count == sends_after_first


def test_request_rejects_changing_strategy():
    """Flipping strategy mid-run would reshuffle bundles under a live
    placement group, so an ongoing request pins it."""
    coord = _make_coordinator(STRATEGY_CLUSTER_NODES)
    coord.request_resources(
        requester_id="train",
        resources=[{"GPU": 1}] * 4,
        expire_after_s=10,
        strategy=ResourceRequestStrategy.PACK,
    )

    with pytest.raises(ValueError, match="Cannot change strategy"):
        coord.request_resources(
            requester_id="train",
            resources=[{"GPU": 1}] * 4,
            expire_after_s=10,
            strategy=ResourceRequestStrategy.SPREAD,
        )

    # The rejected call must leave the existing reservation intact.
    assert coord.get_reserved_resources("train") == {"n1": {"GPU": 4}}


def test_strict_pack_forwards_one_summed_bundle_to_the_autoscaler():
    """The autoscaler has to be told a single node must hold everything, or it
    is free to satisfy the request by adding several small nodes.

    Bundles and selectors are parallel lists that the SDK requires to be the
    same length, so collapsing the bundles has to collapse the selectors too.
    """
    coord = _make_coordinator(STRATEGY_CLUSTER_NODES)
    coord.request_resources(
        requester_id="train",
        resources=[{"GPU": 1, "CPU": 2}] * 3,
        label_selectors=[{"zone": "a"}, {"zone": "a"}, {"market": "spot"}],
        expire_after_s=10,
        strategy=ResourceRequestStrategy.STRICT_PACK,
    )

    call = coord._send_resources_request.call_args
    assert call.args[0] == [{"GPU": 3, "CPU": 6}]
    # One node has to satisfy every bundle's selector, so the collapsed bundle
    # carries all of their constraints.
    assert call.kwargs["label_selectors"] == [{"zone": "a", "market": "spot"}]


def test_tick_survives_a_failing_request():
    """`_tick` runs on a bare thread, so an exception escaping it would
    silently stop autoscaling for every requester on the cluster."""
    failing_send = Mock(side_effect=RuntimeError("autoscaler rejected the request"))
    coord = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=failing_send,
        get_cluster_nodes=lambda: STRATEGY_CLUSTER_NODES,
    )

    coord._tick()

    assert failing_send.called


def test_forced_read_drops_a_node_that_has_since_died():
    """Callers turn reservations into node pins, so ``force`` re-reserves
    against a fresh view rather than handing back a node that is gone."""
    nodes = _nodes(n1={"GPU": 1}, n2={"GPU": 1})
    coord = _make_coordinator(nodes)
    coord.request_resources(
        requester_id="train",
        resources=[{"GPU": 1}] * 2,
        expire_after_s=10,
        strategy=ResourceRequestStrategy.SPREAD,
    )
    assert coord.get_reserved_resources("train") == {
        "n1": {"GPU": 1},
        "n2": {"GPU": 1},
    }

    nodes[1]["Alive"] = False

    # An unforced read is allowed to be stale; a forced one must not be.
    assert coord.get_reserved_resources("train") == {
        "n1": {"GPU": 1},
        "n2": {"GPU": 1},
    }
    assert coord.get_reserved_resources("train", recompute=True) == {"n1": {"GPU": 1}}


def test_strategy_is_per_requester():
    """Two requesters in the same cluster each get their own layout."""
    coord = _make_coordinator(
        [
            {"NodeID": "n1", "Resources": {"GPU": 4}, "Alive": True},
            {"NodeID": "n2", "Resources": {"GPU": 4}, "Alive": True},
        ]
    )
    coord.request_resources(
        requester_id="packer",
        resources=[{"GPU": 1}] * 2,
        expire_after_s=10,
        strategy=ResourceRequestStrategy.PACK,
    )
    coord.request_resources(
        requester_id="spreader",
        resources=[{"GPU": 1}] * 2,
        expire_after_s=10,
        strategy=ResourceRequestStrategy.SPREAD,
    )

    assert coord.get_reserved_resources("packer") == {"n1": {"GPU": 2}}
    # The spreader starts scanning at n1 (which has 2 GPU left after the
    # packer) and then rotates to n2.
    assert coord.get_reserved_resources("spreader") == {
        "n1": {"GPU": 1},
        "n2": {"GPU": 1},
    }


def test_label_selector_disjoint_requesters_dont_cross_talk():
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="train",
        resources=[{"CPU": 4}],
        subcluster_selector={"ray-subcluster": "training"},
        expire_after_s=10,
        request_remaining=STANDARD_RESOURCE_TYPES,
    )
    coord.request_resources(
        requester_id="val",
        resources=[{"CPU": 4}],
        subcluster_selector={"ray-subcluster": "validation"},
        expire_after_s=10,
        request_remaining=STANDARD_RESOURCE_TYPES,
    )

    train = coord.get_reserved_resources("train")
    val = coord.get_reserved_resources("val")
    # Training bucket: 2 x 8 = 16 CPU (4 explicit + 12 leftover), all to train.
    assert sum(v.get("CPU", 0) for v in train.values()) == 16
    # Validation bucket: 4 CPU (4 explicit, 0 leftover of CPU).
    assert sum(v.get("CPU", 0) for v in val.values()) == 4


def test_unlabeled_requester_only_sees_none_bucket():
    """An unlabeled requester is only eligible for nodes in the ``None``
    bucket (no subcluster label). It must not get explicit allocations or
    leftover share from any labeled subcluster, even when it has
    ``request_remaining=STANDARD_RESOURCE_TYPES``."""
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="anon",
        resources=[{"CPU": 1}],
        # No label_selector -> effective subcluster = None -> only the
        # default-labeled node (2 CPU).
        expire_after_s=10,
        request_remaining=STANDARD_RESOURCE_TYPES,
    )

    alloc = coord.get_reserved_resources("anon")
    total_cpu = sum(v.get("CPU", 0) for v in alloc.values())
    assert total_cpu == 2, (
        f"unlabeled requester should only see the 2-CPU None bucket; got "
        f"{total_cpu} (alloc={alloc})"
    )


def test_labeled_and_unlabeled_requesters_are_isolated():
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="train",
        resources=[{"CPU": 1}],
        subcluster_selector={"ray-subcluster": "training"},
        expire_after_s=10,
        request_remaining=STANDARD_RESOURCE_TYPES,
    )
    coord.request_resources(
        requester_id="anon",
        resources=[{"CPU": 1}],
        expire_after_s=10,
        request_remaining=STANDARD_RESOURCE_TYPES,
    )

    train_total = sum(
        v.get("CPU", 0) for v in coord.get_reserved_resources("train").values()
    )
    anon_total = sum(
        v.get("CPU", 0) for v in coord.get_reserved_resources("anon").values()
    )
    # Training bucket: 2 x 8 = 16 CPU; anon gets none of it.
    assert train_total == 16
    # Default bucket: 1 x 2 = 2 CPU; train gets none of it.
    assert anon_total == 2


def test_label_selector_unmatched_yields_no_allocation():
    """A requester whose subcluster has no matching nodes gets no
    allocation this tick."""
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="ghost",
        resources=[{"CPU": 1}],
        subcluster_selector={"ray-subcluster": "nonexistent"},
        expire_after_s=10,
        request_remaining=STANDARD_RESOURCE_TYPES,
    )
    assert coord.get_reserved_resources("ghost") == {}


def test_label_selector_partial_fit_when_demand_exceeds_capacity():
    """When demand exceeds capacity in the matching bucket, only the
    bundles that fit get allocated this tick."""
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="val",
        resources=[{"CPU": 3}, {"CPU": 3}, {"CPU": 3}],
        subcluster_selector={"ray-subcluster": "validation"},
        expire_after_s=10,
    )
    # Validation has one 4-CPU node; only the first 3-CPU bundle fits.
    assert coord.get_reserved_resources("val") == {"n-val-1": {"CPU": 3}}


def test_full_tick_exercises_update_merge_reallocate():
    """A `_tick()` call runs update -> merge_and_forward -> reallocate, so
    a mid-stream node-list change is picked up after the next tick."""
    nodes = [
        {
            "NodeID": "n1",
            "Resources": {"CPU": 4},
            "Labels": {"ray-subcluster": "training"},
            "Alive": True,
        },
    ]
    mock_send = Mock()
    coord = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=mock_send,
        get_cluster_nodes=lambda: nodes,
    )
    coord.request_resources(
        requester_id="train",
        resources=[{"CPU": 1}],
        subcluster_selector={"ray-subcluster": "training"},
        expire_after_s=10,
        request_remaining=STANDARD_RESOURCE_TYPES,
    )
    # Before the join: only 4 CPU in the training bucket; 1 used explicitly,
    # 3 leftover go to train.
    train_total = sum(
        v.get("CPU", 0) for v in coord.get_reserved_resources("train").values()
    )
    assert train_total == 4

    # A new training node joins the cluster.
    nodes.append(
        {
            "NodeID": "n2",
            "Resources": {"CPU": 8},
            "Labels": {"ray-subcluster": "training"},
            "Alive": True,
        }
    )
    # Without a tick, the coordinator still sees the old snapshot.
    coord._tick()
    train_total = sum(
        v.get("CPU", 0) for v in coord.get_reserved_resources("train").values()
    )
    # Now: 4 + 8 = 12 total; 1 explicit + 11 leftover.
    assert train_total == 12


def test_labeled_requester_with_empty_resources_stays_pinned():
    """A labeled requester with empty resources + request_remaining=STANDARD_RESOURCE_TYPES is
    eligible only for leftovers from its own subcluster."""
    coord = _make_coordinator(LABELED_CLUSTER_NODES)

    # Idle "train" requester: no bundles, still affiliated with training
    # via the requester-wide ``label_selector``.
    coord.request_resources(
        requester_id="train_idle",
        resources=[],
        subcluster_selector={"ray-subcluster": "training"},
        expire_after_s=10,
        request_remaining=STANDARD_RESOURCE_TYPES,
    )
    # Active "val" requester: asks for 2 CPU on validation. After explicit
    # allocation, validation has 2 CPU of leftover.
    coord.request_resources(
        requester_id="val_active",
        resources=[{"CPU": 2}],
        subcluster_selector={"ray-subcluster": "validation"},
        expire_after_s=10,
        request_remaining=STANDARD_RESOURCE_TYPES,
    )

    train_idle_alloc = coord.get_reserved_resources("train_idle")
    val_active_alloc = coord.get_reserved_resources("val_active")

    # Training bucket: 2 x 8 = 16 CPU, all leftover, all to train_idle.
    train_idle_cpu = sum(v.get("CPU", 0) for v in train_idle_alloc.values())
    assert train_idle_cpu == 16, (
        f"train_idle should get exactly 16 CPU from training only, got "
        f"{train_idle_cpu} (alloc={train_idle_alloc})"
    )
    # Validation bucket: 4 - 2 explicit = 2 leftover, all to val_active.
    # Total = 2 explicit + 2 leftover = 4.
    val_active_cpu = sum(v.get("CPU", 0) for v in val_active_alloc.values())
    assert val_active_cpu == 4, (
        f"val_active should get 2 explicit + 2 leftover = 4 CPU, got "
        f"{val_active_cpu} (alloc={val_active_alloc})"
    )


def test_proxy_forwards_label_selector_from_init():
    """``DefaultAutoscalingCoordinator`` forwards the ``label_selector``
    it was constructed with on every request, so the actor can store the
    requester's subcluster affiliation."""
    mock_actor = Mock()
    proxy = DefaultAutoscalingCoordinator(
        requester_id="r",
        autoscaling_coordinator_actor=mock_actor,
        subcluster_selector={"ray-subcluster": "training"},
    )
    proxy.request_resources(resources=[{"CPU": 1}, {"CPU": 2}], expire_after_s=10)
    kwargs = mock_actor.request_resources.remote.call_args.kwargs
    assert kwargs["subcluster_selector"] == {"ray-subcluster": "training"}


def test_proxy_forwards_label_selector_on_empty_resources():
    """The proxy carries its ``label_selector`` even on the empty /
    registration path, so the actor keeps the requester pinned to its
    subcluster for remaining-resources eligibility."""
    mock_actor = Mock()
    proxy = DefaultAutoscalingCoordinator(
        requester_id="r",
        autoscaling_coordinator_actor=mock_actor,
        subcluster_selector={"ray-subcluster": "training"},
    )
    proxy.request_resources(
        resources=[], expire_after_s=10, request_remaining=STANDARD_RESOURCE_TYPES
    )
    kwargs = mock_actor.request_resources.remote.call_args.kwargs
    assert kwargs["resources"] == []
    assert kwargs["subcluster_selector"] == {"ray-subcluster": "training"}


def test_proxy_passes_caller_label_selectors_through():
    """If the caller passes per-bundle ``label_selectors``, the proxy
    forwards them as-is (used by callers that want per-bundle
    constraints beyond subcluster, e.g. node pins)."""
    mock_actor = Mock()
    proxy = DefaultAutoscalingCoordinator(
        requester_id="r",
        autoscaling_coordinator_actor=mock_actor,
        subcluster_selector={"ray-subcluster": "training"},
    )
    proxy.request_resources(
        resources=[{"CPU": 1}],
        label_selectors=[{"node_id": "n1"}],
        expire_after_s=10,
    )
    kwargs = mock_actor.request_resources.remote.call_args.kwargs
    assert kwargs["label_selectors"] == [{"node_id": "n1"}]
    assert kwargs["subcluster_selector"] == {"ray-subcluster": "training"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
