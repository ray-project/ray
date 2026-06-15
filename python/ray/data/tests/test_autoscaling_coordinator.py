from unittest.mock import Mock

import pytest

import ray
from ray.cluster_utils import Cluster
from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
    HEAD_NODE_RESOURCE_LABEL,
    DefaultAutoscalingCoordinator,
    _AutoscalingCoordinatorActor,
    _format_resources_for_log,
    get_or_create_autoscaling_coordinator,
)
from ray.data._internal.util import GiB
from ray.tests.conftest import wait_for_condition

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


def test_format_resources_for_log():
    # Two bundles that are identical except for sub-precision differences in
    # object_store_memory (8.96 vs 9.04 GiB), plus custom labels and a GPU: 0
    # entry. They should aggregate into a single entry, with custom/zero
    # resources dropped.
    resources = [
        {
            "CPU": 8,
            "GPU": 0,
            "memory": 32 * GiB,
            "object_store_memory": int(8.96 * GiB),
            "anyscale/cpu_only:true": 1.0,
            "anyscale/region:us-west-2": 1.0,
            "node:10.0.193.159": 1.0,
        },
        {
            "CPU": 8,
            "GPU": 0,
            "memory": 32 * GiB,
            "object_store_memory": int(9.04 * GiB),
            "anyscale/cpu_only:true": 1.0,
            "anyscale/region:us-west-2": 1.0,
            "node:10.0.241.173": 1.0,
        },
        {
            "CPU": 0,
            "GPU": 0,
            "memory": 0,
            "object_store_memory": 0,
            "anyscale/cpu_only:true": 1.0,
            "node:10.0.252.14": 1.0,
        },
    ]

    log_message = _format_resources_for_log(resources)
    assert log_message == (
        "[2 x {CPU: 8, memory: 32.0GiB, object_store_memory: 9.0GiB}]"
    )
    assert "anyscale/" not in log_message
    assert "node:" not in log_message
    assert "GPU" not in log_message


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


def test_get_allocated_resources_eventually_consistent(autoscaling_coordinator_actor):
    """get_allocated_resources eventually reflects a submitted request_resources call."""
    coordinator = DefaultAutoscalingCoordinator(
        "test", autoscaling_coordinator_actor=autoscaling_coordinator_actor
    )

    coordinator.request_resources(resources=[{"CPU": 1}], expire_after_s=60)

    wait_for_condition(
        lambda: coordinator.get_allocated_resources() == [{"CPU": 1}],
        retry_interval_ms=100,
        timeout=5,
    )


def test_get_allocated_resources_returns_cached_while_pending(
    autoscaling_coordinator_actor, monkeypatch
):
    """Returns the last cached value without blocking when a ref is still in-flight."""
    coordinator = DefaultAutoscalingCoordinator(
        "test", autoscaling_coordinator_actor=autoscaling_coordinator_actor
    )

    coordinator.request_resources(resources=[{"CPU": 1}], expire_after_s=60)
    wait_for_condition(
        lambda: coordinator.get_allocated_resources() == [{"CPU": 1}],
        retry_interval_ms=100,
        timeout=5,
    )

    # Make ray.wait report all refs as still pending.
    def fake_wait(refs, *args, **kwargs):
        return [], refs

    monkeypatch.setattr(ray, "wait", fake_wait)

    coordinator.request_resources(resources=[{"CPU": 2}], expire_after_s=60)
    # Should return the stale cached value, not block.
    assert coordinator.get_allocated_resources() == [{"CPU": 1}]


def test_get_allocated_resources_returns_cached_on_actor_error(
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
        lambda: coordinator.get_allocated_resources() == [{"CPU": 1}],
        retry_interval_ms=100,
        timeout=5,
    )

    def fake_wait(refs, *args, **kwargs):
        # Report the ref as ready so ray.get is attempted.
        return refs, []

    monkeypatch.setattr(ray, "wait", fake_wait)
    monkeypatch.setattr(ray, "get", Mock(side_effect=ray.exceptions.RayActorError()))

    # Must return the last cached value, not raise.
    assert coordinator.get_allocated_resources() == [{"CPU": 1}]

    # Recovery: submit a new request after the error and verify it eventually
    # resolves, proving the coordinator can communicate with the actor again.
    monkeypatch.undo()
    coordinator.request_resources(resources=[{"CPU": 2}], expire_after_s=60)
    wait_for_condition(
        lambda: coordinator.get_allocated_resources() == [{"CPU": 2}],
        retry_interval_ms=100,
        timeout=5,
    )


def test_cancel_request_makes_get_return_empty(autoscaling_coordinator_actor):
    """After cancel_request, get_allocated_resources eventually returns []."""
    coordinator = DefaultAutoscalingCoordinator(
        "test", autoscaling_coordinator_actor=autoscaling_coordinator_actor
    )

    coordinator.request_resources(resources=[{"CPU": 1}], expire_after_s=60)
    wait_for_condition(
        lambda: coordinator.get_allocated_resources() == [{"CPU": 1}],
        retry_interval_ms=100,
        timeout=5,
    )

    coordinator.cancel_request()
    wait_for_condition(
        lambda: coordinator.get_allocated_resources() == [],
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
        lambda: coordinator.get_allocated_resources() == [{"CPU": 1}],
        retry_interval_ms=100,
        timeout=5,
    )

    monkeypatch.setattr(ray, "wait", lambda refs, *a, **kw: (refs, []))
    monkeypatch.setattr(
        ray, "get", Mock(side_effect=ValueError("unexpected local error"))
    )

    with pytest.raises(ValueError, match="unexpected local error"):
        coordinator.get_allocated_resources()


def test_coordinator_accepts_zero_resource_for_missing_resource_type(
    autoscaling_coordinator_actor,
):
    # This is a regression test for a bug where the coordinator crashes when you request
    # a resource type (e.g., GPU: 0) that doesn't exist on the cluster.
    coordinator = DefaultAutoscalingCoordinator(
        "spam", autoscaling_coordinator_actor=autoscaling_coordinator_actor
    )

    coordinator.request_resources(resources=[{"CPU": 1, "GPU": 0}], expire_after_s=1)

    wait_for_condition(
        lambda: coordinator.get_allocated_resources() == [{"CPU": 1, "GPU": 0}],
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
        subcluster_selector={"__subcluster__": "training"},
        expire_after_s=10,
    )
    mock_send.assert_called_once_with(
        [{"CPU": 1}, {"CPU": 1}],
        label_selectors=[
            {"node_id": "n1", "__subcluster__": "training"},
            {"__subcluster__": "training"},
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
                {"__subcluster__": "training"},
                {"__subcluster__": "validation"},
            ],
            subcluster_selector={"__subcluster__": "training"},
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
        subcluster_selector={"__subcluster__": "training"},
        expire_after_s=10,
    )
    with pytest.raises(ValueError, match="Cannot change subcluster_selector"):
        coord.request_resources(
            requester_id="r",
            resources=[{"CPU": 1}],
            subcluster_selector={"__subcluster__": "validation"},
            expire_after_s=10,
        )
    # Registry must be unchanged after the rejected call.
    assert coord._subcluster_selectors["r"] == {"__subcluster__": "training"}


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
        "Labels": {"__subcluster__": "training"},
        "Alive": True,
    },
    {
        "NodeID": "n-train-2",
        "Resources": {"CPU": 8, "object_store_memory": 1000},
        "Labels": {"__subcluster__": "training"},
        "Alive": True,
    },
    {
        "NodeID": "n-val-1",
        "Resources": {"CPU": 4, "object_store_memory": 500},
        "Labels": {"__subcluster__": "validation"},
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


def test_label_selector_disjoint_requesters_dont_cross_talk():
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="train",
        resources=[{"CPU": 4}],
        subcluster_selector={"__subcluster__": "training"},
        expire_after_s=10,
        request_remaining=True,
    )
    coord.request_resources(
        requester_id="val",
        resources=[{"CPU": 4}],
        subcluster_selector={"__subcluster__": "validation"},
        expire_after_s=10,
        request_remaining=True,
    )

    train = coord.get_allocated_resources("train")
    val = coord.get_allocated_resources("val")
    assert {"CPU": 4} in train and {"CPU": 4} in val
    # Training bucket: 2 x 8 - 4 explicit = 12 leftover, all to train.
    assert sum(a["CPU"] for a in train if a != {"CPU": 4}) == 12
    # Validation bucket: 4 - 4 explicit = 0 leftover.
    assert sum(a["CPU"] for a in val if a != {"CPU": 4}) == 0


def test_unlabeled_requester_only_sees_none_bucket():
    """An unlabeled requester is only eligible for nodes in the ``None``
    bucket (no subcluster label). It must not get explicit allocations or
    leftover share from any labeled subcluster, even when it has
    ``request_remaining=True``."""
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="anon",
        resources=[{"CPU": 1}],
        # No label_selector -> effective subcluster = None -> only the
        # default-labeled node (2 CPU).
        expire_after_s=10,
        request_remaining=True,
    )

    alloc = coord.get_allocated_resources("anon")
    total_cpu = sum(a["CPU"] for a in alloc)
    assert total_cpu == 2, (
        f"unlabeled requester should only see the 2-CPU None bucket; got "
        f"{total_cpu} (alloc={alloc})"
    )


def test_labeled_and_unlabeled_requesters_are_isolated():
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="train",
        resources=[{"CPU": 1}],
        subcluster_selector={"__subcluster__": "training"},
        expire_after_s=10,
        request_remaining=True,
    )
    coord.request_resources(
        requester_id="anon",
        resources=[{"CPU": 1}],
        expire_after_s=10,
        request_remaining=True,
    )

    train_total = sum(a["CPU"] for a in coord.get_allocated_resources("train"))
    anon_total = sum(a["CPU"] for a in coord.get_allocated_resources("anon"))
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
        subcluster_selector={"__subcluster__": "nonexistent"},
        expire_after_s=10,
        request_remaining=True,
    )
    assert coord.get_allocated_resources("ghost") == []


def test_label_selector_partial_fit_when_demand_exceeds_capacity():
    """When demand exceeds capacity in the matching bucket, only the
    bundles that fit get allocated this tick."""
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="val",
        resources=[{"CPU": 3}, {"CPU": 3}, {"CPU": 3}],
        subcluster_selector={"__subcluster__": "validation"},
        expire_after_s=10,
    )
    # Validation has one 4-CPU node; only the first 3-CPU bundle fits.
    assert coord.get_allocated_resources("val") == [{"CPU": 3}]


def test_full_tick_exercises_update_merge_reallocate():
    """A `_tick()` call runs update -> merge_and_forward -> reallocate, so
    a mid-stream node-list change is picked up after the next tick."""
    nodes = [
        {
            "NodeID": "n1",
            "Resources": {"CPU": 4},
            "Labels": {"__subcluster__": "training"},
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
        subcluster_selector={"__subcluster__": "training"},
        expire_after_s=10,
        request_remaining=True,
    )
    # Before the join: only 4 CPU in the training bucket; 1 used explicitly,
    # 3 leftover go to train.
    train_total = sum(a["CPU"] for a in coord.get_allocated_resources("train"))
    assert train_total == 4

    # A new training node joins the cluster.
    nodes.append(
        {
            "NodeID": "n2",
            "Resources": {"CPU": 8},
            "Labels": {"__subcluster__": "training"},
            "Alive": True,
        }
    )
    # Without a tick, the coordinator still sees the old snapshot.
    coord._tick()
    train_total = sum(a["CPU"] for a in coord.get_allocated_resources("train"))
    # Now: 4 + 8 = 12 total; 1 explicit + 11 leftover.
    assert train_total == 12


def test_labeled_requester_with_empty_resources_stays_pinned():
    """A labeled requester with empty resources + request_remaining=True is
    eligible only for leftovers from its own subcluster."""
    coord = _make_coordinator(LABELED_CLUSTER_NODES)

    # Idle "train" requester: no bundles, still affiliated with training
    # via the requester-wide ``label_selector``.
    coord.request_resources(
        requester_id="train_idle",
        resources=[],
        subcluster_selector={"__subcluster__": "training"},
        expire_after_s=10,
        request_remaining=True,
    )
    # Active "val" requester: asks for 2 CPU on validation. After explicit
    # allocation, validation has 2 CPU of leftover.
    coord.request_resources(
        requester_id="val_active",
        resources=[{"CPU": 2}],
        subcluster_selector={"__subcluster__": "validation"},
        expire_after_s=10,
        request_remaining=True,
    )

    train_idle_alloc = coord.get_allocated_resources("train_idle")
    val_active_alloc = coord.get_allocated_resources("val_active")

    # Training bucket: 2 x 8 = 16 CPU, all leftover, all to train_idle.
    train_idle_cpu = sum(a.get("CPU", 0) for a in train_idle_alloc)
    assert train_idle_cpu == 16, (
        f"train_idle should get exactly 16 CPU from training only, got "
        f"{train_idle_cpu} (alloc={train_idle_alloc})"
    )
    # Validation bucket: 4 - 2 explicit = 2 leftover, all to val_active.
    # Total = 2 explicit + 2 leftover = 4.
    val_active_cpu = sum(a["CPU"] for a in val_active_alloc)
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
        subcluster_selector={"__subcluster__": "training"},
    )
    proxy.request_resources(resources=[{"CPU": 1}, {"CPU": 2}], expire_after_s=10)
    kwargs = mock_actor.request_resources.remote.call_args.kwargs
    assert kwargs["subcluster_selector"] == {"__subcluster__": "training"}


def test_proxy_forwards_label_selector_on_empty_resources():
    """The proxy carries its ``label_selector`` even on the empty /
    registration path, so the actor keeps the requester pinned to its
    subcluster for remaining-resources eligibility."""
    mock_actor = Mock()
    proxy = DefaultAutoscalingCoordinator(
        requester_id="r",
        autoscaling_coordinator_actor=mock_actor,
        subcluster_selector={"__subcluster__": "training"},
    )
    proxy.request_resources(resources=[], expire_after_s=10, request_remaining=True)
    kwargs = mock_actor.request_resources.remote.call_args.kwargs
    assert kwargs["resources"] == []
    assert kwargs["subcluster_selector"] == {"__subcluster__": "training"}


def test_proxy_passes_caller_label_selectors_through():
    """If the caller passes per-bundle ``label_selectors``, the proxy
    forwards them as-is (used by callers that want per-bundle
    constraints beyond subcluster, e.g. node pins)."""
    mock_actor = Mock()
    proxy = DefaultAutoscalingCoordinator(
        requester_id="r",
        autoscaling_coordinator_actor=mock_actor,
        subcluster_selector={"__subcluster__": "training"},
    )
    proxy.request_resources(
        resources=[{"CPU": 1}],
        label_selectors=[{"node_id": "n1"}],
        expire_after_s=10,
    )
    kwargs = mock_actor.request_resources.remote.call_args.kwargs
    assert kwargs["label_selectors"] == [{"node_id": "n1"}]
    assert kwargs["subcluster_selector"] == {"__subcluster__": "training"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
