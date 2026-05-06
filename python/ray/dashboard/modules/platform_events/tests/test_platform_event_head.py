"""Unit tests for PlatformEventsHead.

The k8s event object is mocked with MagicMock. Tests call module methods
directly to keep them fast and hermetic.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.platform_event_pb2 import Source
from ray.dashboard.modules.platform_events.platform_event_head import (
    MAX_EVENTS_TO_CACHE,
    PlatformEventsHead,
)
from ray.dashboard.utils import DashboardHeadModuleConfig


def _make_config() -> DashboardHeadModuleConfig:
    return DashboardHeadModuleConfig(
        minimal=False,
        cluster_id_hex="deadbeef",
        session_name="test_session",
        gcs_address="127.0.0.1:6379",
        log_dir="/tmp",
        temp_dir="/tmp",
        session_dir="/tmp",
        ip="127.0.0.1",
        http_host="127.0.0.1",
        http_port=8265,
    )


def _make_head(**kwargs) -> PlatformEventsHead:
    head = PlatformEventsHead(_make_config())
    for k, v in kwargs.items():
        setattr(head, k, v)
    return head


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
    head = _make_head()
    result = await head._init_k8s_client()
    assert result is False
    assert head._k8s_v1_api is None


def test_k8s_event_update():
    head = _make_head()
    evt = _make_k8s_event(message="some message")
    evt.metadata.uid = "test_uid"
    evt.count = 1

    head._process_k8s_event_callback(evt)
    assert len(head._events) == 1

    ray_event = next(iter(head._events.values()))
    assert ray_event.message == "some message"
    assert "count" not in ray_event.platform_event.custom_fields

    evt2 = _make_k8s_event()
    evt2.metadata.uid = "test_uid"
    evt2.count = 5
    evt2.message = "updated message"

    head._process_k8s_event_callback(evt2)
    assert len(head._events) == 1

    ray_event_updated = next(iter(head._events.values()))
    assert ray_event_updated.message == "updated message"
    assert ray_event_updated.platform_event.custom_fields["count"] == "5"


def test_proto_event_type_and_source_type():
    head = _make_head()
    evt = _make_k8s_event()
    head._process_k8s_event_callback(evt)

    assert len(head._events) == 1
    ray_event = next(iter(head._events.values()))
    assert ray_event.event_type == RayEvent.EventType.PLATFORM_EVENT
    assert ray_event.source_type == RayEvent.SourceType.CLUSTER_LIFECYCLE


def test_platform_source_is_kubernetes():
    head = _make_head()
    evt = _make_k8s_event(component="kubelet")
    head._process_k8s_event_callback(evt)

    source = next(iter(head._events.values())).platform_event.source
    assert source.platform == Source.Platform.KUBERNETES
    assert source.component == "kubelet"
    assert source.metadata["namespace"] == "default"


def test_platform_event_object_fields():
    head = _make_head(_cluster_name="prod-cluster", _ray_job_name="my-job")
    evt = _make_k8s_event(
        uid="uid-abc-123",
        kind="RayJob",
        name="my-job",
        message="Job failed",
        reason="BackoffLimitExceeded",
    )
    head._process_k8s_event_callback(evt)

    ray_event = next(iter(head._events.values()))
    assert ray_event.event_id == b"uid-abc-123"

    pe = ray_event.platform_event
    assert pe.object_kind == "RayJob"
    assert pe.object_name == "my-job"
    assert pe.message == "Job failed"
    assert pe.reason == "BackoffLimitExceeded"
    assert pe.source.metadata["ray_cluster_name"] == "prod-cluster"


@pytest.mark.parametrize(
    "event_type,expected_severity",
    [
        ("Warning", RayEvent.Severity.WARNING),
        ("Normal", RayEvent.Severity.INFO),
        (None, RayEvent.Severity.INFO),
    ],
)
def test_severity_mapping(event_type, expected_severity):
    head = _make_head()
    evt = _make_k8s_event(event_type=event_type)
    head._process_k8s_event_callback(evt)
    assert next(iter(head._events.values())).severity == expected_severity


def test_timestamp_uses_last_timestamp():
    head = _make_head()
    ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    evt = _make_k8s_event(
        last_timestamp=ts, first_timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc)
    )
    head._process_k8s_event_callback(evt)

    ray_event = next(iter(head._events.values()))
    assert ray_event.timestamp.seconds == int(ts.timestamp())


def test_timestamp_falls_back_to_first_timestamp():
    head = _make_head()
    ts = datetime(2025, 3, 15, 8, 30, 0, tzinfo=timezone.utc)
    evt = _make_k8s_event(last_timestamp=None, first_timestamp=ts)
    head._process_k8s_event_callback(evt)

    ray_event = next(iter(head._events.values()))
    assert ray_event.timestamp.seconds == int(ts.timestamp())


def test_dedup_suppresses_repeated_uid():
    head = _make_head()
    evt = _make_k8s_event(uid="uid-repeated")
    head._process_k8s_event_callback(evt)
    head._process_k8s_event_callback(evt)

    assert len(head._events) == 1


def test_dedup_uid_eviction_keeps_recent():
    """Verify that cache size does not exceed MAX_EVENTS_TO_CACHE and keeps recent items."""
    head = _make_head()
    total = MAX_EVENTS_TO_CACHE + 10
    for i in range(total):
        head._process_k8s_event_callback(_make_k8s_event(uid=f"uid-{i}"))

    assert len(head._events) == MAX_EVENTS_TO_CACHE
    # Oldest UIDs should be gone
    assert "uid-0" not in head._events
    # Newest should still be there
    assert f"uid-{total - 1}" in head._events


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
