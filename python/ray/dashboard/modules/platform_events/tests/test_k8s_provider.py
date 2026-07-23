"""Unit tests for KubernetesEventProvider.

Tests verify that raw Kubernetes events are correctly parsed, translated,
and mapped to standardized RayEvent protobuf structures.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from kubernetes.client.rest import ApiException

from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.platform_event_pb2 import Source
from ray.dashboard.modules.platform_events.providers.k8s_provider import (
    KUBERAY_LABEL_KEY_CLUSTER,
    MAX_NON_CLUSTER_POD_CACHE,
    KubernetesEventProvider,
)


def _make_k8s_event(
    uid: str = "uid-abc-123",
    kind: str = "RayCluster",
    name: str = "my-cluster",
    message: str = "Pod started",
    reason: str = "Started",
    event_type: str = "Normal",
    namespace: str = "default",
    count: int = 1,
    last_timestamp: datetime = None,
    first_timestamp: datetime = None,
    event_timestamp: datetime = None,
    component: str = "kubelet",
) -> MagicMock:
    evt = MagicMock()
    evt.metadata.uid = uid
    evt.metadata.namespace = namespace
    evt.metadata.resource_version = "12345"
    evt.involved_object.kind = kind
    evt.involved_object.name = name
    evt.message = message
    evt.reason = reason
    evt.type = event_type
    evt.count = count
    evt.last_timestamp = last_timestamp
    evt.first_timestamp = first_timestamp
    evt.event_time = event_timestamp
    evt.source.component = component
    return evt


@pytest.mark.asyncio
async def test_init_k8s_client_missing_cluster_name():
    provider = KubernetesEventProvider(lambda e: None)
    result = await provider._init_k8s_client()
    assert result is False


@pytest.mark.asyncio
async def test_init_k8s_client_missing_cluster_namespace(monkeypatch):
    monkeypatch.setenv("RAY_CLUSTER_NAME", "my-cluster")
    monkeypatch.delenv("RAY_CLUSTER_NAMESPACE", raising=False)

    provider = KubernetesEventProvider(lambda e: None)
    result = await provider._init_k8s_client()
    assert result is False


def test_proto_event_type_and_source_type():
    delivered_event = None

    def callback(event: RayEvent):
        nonlocal delivered_event
        delivered_event = event

    provider = KubernetesEventProvider(callback)
    provider._cluster_name = "my-cluster"

    evt = _make_k8s_event()
    provider._process_k8s_event(evt)

    assert delivered_event is not None
    assert delivered_event.event_type == RayEvent.EventType.PLATFORM_EVENT
    assert delivered_event.source_type == RayEvent.SourceType.CLUSTER_LIFECYCLE


def test_platform_source_is_kubernetes():
    delivered_event = None

    def callback(event: RayEvent):
        nonlocal delivered_event
        delivered_event = event

    provider = KubernetesEventProvider(callback)
    provider._cluster_name = "prod-cluster"

    evt = _make_k8s_event(component="kubelet", namespace="my-namespace")
    provider._process_k8s_event(evt)

    assert delivered_event is not None
    source = delivered_event.platform_event.source
    assert source.platform == Source.Platform.KUBERNETES
    assert source.component == "kubelet"
    assert source.metadata["namespace"] == "my-namespace"
    assert source.metadata["ray_cluster_name"] == "prod-cluster"


def test_platform_event_object_fields():
    delivered_event = None

    def callback(event: RayEvent):
        nonlocal delivered_event
        delivered_event = event

    provider = KubernetesEventProvider(callback)
    evt = _make_k8s_event(
        uid="uid-abc-123",
        kind="RayJob",
        name="my-job",
        message="Job failed",
        reason="BackoffLimitExceeded",
    )
    provider._process_k8s_event(evt)

    assert delivered_event is not None
    assert delivered_event.event_id == b"uid-abc-123"

    pe = delivered_event.platform_event
    assert pe.object_kind == "RayJob"
    assert pe.object_name == "my-job"
    assert pe.message == "Job failed"
    assert pe.reason == "BackoffLimitExceeded"


@pytest.mark.parametrize(
    "event_type,expected_severity",
    [
        ("Warning", RayEvent.Severity.WARNING),
        ("Normal", RayEvent.Severity.INFO),
        (None, RayEvent.Severity.INFO),
    ],
)
def test_severity_mapping(event_type, expected_severity):
    delivered_event = None

    def callback(event: RayEvent):
        nonlocal delivered_event
        delivered_event = event

    provider = KubernetesEventProvider(callback)
    evt = _make_k8s_event(event_type=event_type)
    provider._process_k8s_event(evt)

    assert delivered_event is not None
    assert delivered_event.severity == expected_severity


def test_timestamp_uses_last_timestamp():
    delivered_event = None

    def callback(event: RayEvent):
        nonlocal delivered_event
        delivered_event = event

    provider = KubernetesEventProvider(callback)
    ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    evt = _make_k8s_event(
        last_timestamp=ts, first_timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc)
    )
    provider._process_k8s_event(evt)

    assert delivered_event is not None
    assert delivered_event.timestamp.seconds == int(ts.timestamp())


def test_timestamp_falls_back_to_first_timestamp():
    delivered_event = None

    def callback(event: MagicMock):
        nonlocal delivered_event
        delivered_event = event

    provider = KubernetesEventProvider(callback)
    ts = datetime(2025, 3, 15, 8, 30, 0, tzinfo=timezone.utc)
    evt = _make_k8s_event(last_timestamp=None, first_timestamp=ts)
    provider._process_k8s_event(evt)

    assert delivered_event is not None
    assert delivered_event.timestamp.seconds == int(ts.timestamp())


@pytest.mark.parametrize(
    "event_type,expected_present",
    [
        ("ADDED", True),
        ("MODIFIED", True),
        ("DELETED", False),
    ],
)
def test_update_pod_membership(event_type, expected_present):
    provider = KubernetesEventProvider(lambda e: None)
    # Seed so DELETED has something to remove.
    provider._cluster_pod_names.add("ray-worker-0")

    provider._update_pod_membership(event_type, "ray-worker-0")

    assert ("ray-worker-0" in provider._cluster_pod_names) is expected_present


def test_update_pod_membership_delete_unknown_is_noop():
    provider = KubernetesEventProvider(lambda e: None)
    provider._update_pod_membership("DELETED", "never-added")
    assert "never-added" not in provider._cluster_pod_names


def test_relist_pod_membership_replaces_cache():
    provider = KubernetesEventProvider(lambda e: None)
    provider._namespace = "ns"

    provider._cluster_pod_names = {"stale-pod", "ray-worker-0"}

    live = MagicMock()
    live.metadata.resource_version = "999"
    live.items = [MagicMock(), MagicMock()]
    live.items[0].metadata.name = "ray-worker-0"
    live.items[1].metadata.name = "ray-worker-1"
    provider._k8s_v1_api = MagicMock()
    provider._k8s_v1_api.list_namespaced_pod.return_value = live

    rv = provider._relist_pod_membership(f"{KUBERAY_LABEL_KEY_CLUSTER}=foo")

    assert rv == "999"
    assert provider._cluster_pod_names == {"ray-worker-0", "ray-worker-1"}
    assert "stale-pod" not in provider._cluster_pod_names


def test_pod_event_dispatched_only_for_cluster_pods():
    delivered = []

    def callback(event: RayEvent):
        delivered.append(event)

    provider = KubernetesEventProvider(callback)
    provider._cluster_name = "my-cluster"
    provider._update_pod_membership("ADDED", "ray-worker-0")
    provider._k8s_v1_api = MagicMock()
    provider._k8s_v1_api.read_namespaced_pod.return_value = _make_pod(
        "some-other-pod", "other-cluster"
    )

    # Event for a pod in this cluster — should be delivered.
    in_cluster_evt = _make_k8s_event(
        uid="uid-1", kind="Pod", name="ray-worker-0", reason="Scheduled"
    )
    if provider._resolve_cluster_pod(in_cluster_evt.involved_object.name):
        provider._process_k8s_event(in_cluster_evt)

    # Event for a pod outside this cluster — should be filtered out.
    other_evt = _make_k8s_event(
        uid="uid-2", kind="Pod", name="some-other-pod", reason="Scheduled"
    )
    if provider._resolve_cluster_pod(other_evt.involved_object.name):
        provider._process_k8s_event(other_evt)

    assert len(delivered) == 1
    assert delivered[0].event_id == b"uid-1"
    assert delivered[0].platform_event.object_kind == "Pod"
    assert delivered[0].platform_event.object_name == "ray-worker-0"


def _make_pod(name: str, cluster_label: str) -> MagicMock:
    pod = MagicMock()
    pod.metadata.name = name
    pod.metadata.labels = (
        {KUBERAY_LABEL_KEY_CLUSTER: cluster_label} if cluster_label is not None else {}
    )
    return pod


def test_resolve_cluster_pod_positive_cache_hit_skips_get():
    provider = KubernetesEventProvider(lambda e: None)
    provider._k8s_v1_api = MagicMock()
    provider._cluster_pod_names.add("ray-worker-0")

    assert provider._resolve_cluster_pod("ray-worker-0") is True
    provider._k8s_v1_api.read_namespaced_pod.assert_not_called()


def test_resolve_cluster_pod_negative_cache_hit_skips_get():
    provider = KubernetesEventProvider(lambda e: None)
    provider._k8s_v1_api = MagicMock()
    provider._non_cluster_pod_names["other-pod"] = None

    assert provider._resolve_cluster_pod("other-pod") is False
    provider._k8s_v1_api.read_namespaced_pod.assert_not_called()


def test_resolve_cluster_pod_cold_miss_matching_label_caches_positive():
    provider = KubernetesEventProvider(lambda e: None)
    provider._cluster_name = "my-cluster"
    provider._namespace = "ns"
    provider._k8s_v1_api = MagicMock()
    provider._k8s_v1_api.read_namespaced_pod.return_value = _make_pod(
        "ray-worker-1", "my-cluster"
    )

    assert provider._resolve_cluster_pod("ray-worker-1") is True
    assert "ray-worker-1" in provider._cluster_pod_names
    # A second resolution is served from cache without re-issuing the GET.
    provider._resolve_cluster_pod("ray-worker-1")
    provider._k8s_v1_api.read_namespaced_pod.assert_called_once()


def test_resolve_cluster_pod_cold_miss_not_matching_label_caches_negative():
    provider = KubernetesEventProvider(lambda e: None)
    provider._cluster_name = "my-cluster"
    provider._namespace = "ns"
    provider._k8s_v1_api = MagicMock()
    provider._k8s_v1_api.read_namespaced_pod.return_value = _make_pod(
        "other-pod", "other-cluster"
    )
    assert provider._resolve_cluster_pod("other-pod") is False
    assert "other-pod" in provider._non_cluster_pod_names
    provider._resolve_cluster_pod("other-pod")
    provider._k8s_v1_api.read_namespaced_pod.assert_called_once()


def test_resolve_cluster_pod_deleted_pod_counts_as_not_member():
    provider = KubernetesEventProvider(lambda e: None)
    provider._cluster_name = "my-cluster"
    provider._namespace = "ns"
    provider._k8s_v1_api = MagicMock()
    provider._k8s_v1_api.read_namespaced_pod.side_effect = ApiException(
        status=404, reason="Not Found"
    )
    assert provider._resolve_cluster_pod("gone-pod") is False
    assert "gone-pod" in provider._non_cluster_pod_names


def test_resolve_cluster_pod_negative_cache_is_bounded():
    provider = KubernetesEventProvider(lambda e: None)
    provider._cluster_name = "my-cluster"
    provider._namespace = "ns"
    provider._k8s_v1_api = MagicMock()
    provider._k8s_v1_api.read_namespaced_pod.side_effect = (
        lambda name, namespace, _request_timeout: _make_pod(name, "other-cluster")
    )

    for i in range(MAX_NON_CLUSTER_POD_CACHE + 5):
        provider._resolve_cluster_pod(f"other-pod-{i}")

    assert len(provider._non_cluster_pod_names) <= MAX_NON_CLUSTER_POD_CACHE
    assert "other-pod-0" not in provider._non_cluster_pod_names
    assert (
        f"other-pod-{MAX_NON_CLUSTER_POD_CACHE + 4}" in provider._non_cluster_pod_names
    )


def test_membership_add_or_delete_purges_negative_cache_entry():
    provider = KubernetesEventProvider(lambda e: None)

    provider._non_cluster_pod_names["ray-worker-2"] = None
    provider._update_pod_membership("ADDED", "ray-worker-2")
    assert "ray-worker-2" not in provider._non_cluster_pod_names

    provider._non_cluster_pod_names["ray-worker-3"] = None
    provider._update_pod_membership("DELETED", "ray-worker-3")
    assert "ray-worker-3" not in provider._non_cluster_pod_names


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
