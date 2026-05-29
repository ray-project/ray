from unittest.mock import Mock

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
    """Test that label selectors are forwarded to the autoscaler SDK."""
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
        "Labels": {"subcluster": "training"},
        "Alive": True,
    },
    {
        "NodeID": "n-train-2",
        "Resources": {"CPU": 8, "object_store_memory": 1000},
        "Labels": {"subcluster": "training"},
        "Alive": True,
    },
    {
        "NodeID": "n-val-1",
        "Resources": {"CPU": 4, "object_store_memory": 500},
        "Labels": {"subcluster": "validation"},
        "Alive": True,
    },
    {
        "NodeID": "n-default-1",
        "Resources": {"CPU": 2, "object_store_memory": 200},
        "Labels": {},
        "Alive": True,
    },
]


def _make_coordinator(nodes, subcluster_label_key="subcluster"):
    return _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=Mock(),
        get_cluster_nodes=lambda: nodes,
        subcluster_label_key=subcluster_label_key,
    )


def test_label_selector_disjoint_requesters_dont_cross_talk():
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="train",
        resources=[{"CPU": 4}],
        label_selectors=[{"subcluster": "training"}],
        expire_after_s=10,
        request_remaining=True,
    )
    coord.request_resources(
        requester_id="val",
        resources=[{"CPU": 4}],
        label_selectors=[{"subcluster": "validation"}],
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


def test_unlabeled_requester_cannot_use_labeled_nodes():
    """An unlabeled requester's bundles fall in the ``None`` bucket and
    must not consume capacity in any named subcluster."""
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="anon",
        resources=[{"CPU": 1}],
        # No label_selectors -> bucket = None -> only the default-labeled node.
        expire_after_s=10,
        request_remaining=True,
    )

    alloc = coord.get_allocated_resources("anon")
    total_cpu = sum(a["CPU"] for a in alloc)
    # Only the default-bucket node (2 CPU) is reachable.
    assert total_cpu == 2


def test_labeled_and_unlabeled_requesters_are_isolated():
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="train",
        resources=[{"CPU": 1}],
        label_selectors=[{"subcluster": "training"}],
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
    """A bundle whose bucket has no matching nodes gets no allocation this
    tick; the hint is still forwarded so the autoscaler can react."""
    mock_send = Mock()
    coord = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0,
        send_resources_request=mock_send,
        get_cluster_nodes=lambda: LABELED_CLUSTER_NODES,
    )
    coord.request_resources(
        requester_id="ghost",
        resources=[{"CPU": 1}],
        label_selectors=[{"subcluster": "nonexistent"}],
        expire_after_s=10,
        request_remaining=True,
    )
    assert coord.get_allocated_resources("ghost") == []
    mock_send.assert_called_with(
        [{"CPU": 1}], label_selectors=[{"subcluster": "nonexistent"}]
    )


def test_label_selector_partial_fit_when_demand_exceeds_capacity():
    """When demand exceeds capacity in the matching bucket, only the
    bundles that fit get allocated this tick."""
    coord = _make_coordinator(LABELED_CLUSTER_NODES)
    coord.request_resources(
        requester_id="val",
        resources=[{"CPU": 3}, {"CPU": 3}, {"CPU": 3}],
        label_selectors=[{"subcluster": "validation"}] * 3,
        expire_after_s=10,
    )
    # Validation has one 4-CPU node; only the first 3-CPU bundle fits.
    assert coord.get_allocated_resources("val") == [{"CPU": 3}]


def test_configurable_subcluster_label_key():
    """Bucketing keys on the configured label, not on a hardcoded one."""
    nodes = [
        {
            "NodeID": "n1",
            "Resources": {"CPU": 4},
            "Labels": {"tier": "spot"},
            "Alive": True,
        },
        {
            "NodeID": "n2",
            "Resources": {"CPU": 4},
            "Labels": {"tier": "on-demand"},
            "Alive": True,
        },
    ]
    coord = _make_coordinator(nodes, subcluster_label_key="tier")
    coord.request_resources(
        requester_id="r",
        resources=[{"CPU": 4}],
        label_selectors=[{"tier": "spot"}],
        expire_after_s=10,
    )
    # Only the spot node matches the "tier=spot" bucket.
    assert coord.get_allocated_resources("r") == [{"CPU": 4}]


def test_full_tick_exercises_update_merge_reallocate():
    """A `_tick()` call runs update -> merge_and_forward -> reallocate, so
    a mid-stream node-list change is picked up after the next tick."""
    nodes = [
        {
            "NodeID": "n1",
            "Resources": {"CPU": 4},
            "Labels": {"subcluster": "training"},
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
        label_selectors=[{"subcluster": "training"}],
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
            "Labels": {"subcluster": "training"},
            "Alive": True,
        }
    )
    # Without a tick, the coordinator still sees the old snapshot.
    coord._tick()
    train_total = sum(a["CPU"] for a in coord.get_allocated_resources("train"))
    # Now: 4 + 8 = 12 total; 1 explicit + 11 leftover.
    assert train_total == 12


def test_get_or_create_raises_on_mismatched_subcluster_label_key(
    ray_start_regular_shared,
):
    """Same key is reusable; a different key raises."""
    import ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator as dac

    dac._AGREED_SUBCLUSTER_LABEL_KEY = None
    try:
        get_or_create_autoscaling_coordinator(subcluster_label_key="subcluster")
        get_or_create_autoscaling_coordinator(subcluster_label_key="subcluster")
        with pytest.raises(ValueError, match="subcluster_label_key"):
            get_or_create_autoscaling_coordinator(subcluster_label_key="tier")
    finally:
        dac._AGREED_SUBCLUSTER_LABEL_KEY = None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
