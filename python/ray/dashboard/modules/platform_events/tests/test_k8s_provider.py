"""Unit tests for KubernetesEventProvider.

Tests verify that raw Kubernetes events are correctly parsed, translated,
and mapped to standardized RayEvent protobuf structures.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.platform_event_pb2 import Source
from ray.dashboard.modules.platform_events.providers.k8s_provider import (
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
