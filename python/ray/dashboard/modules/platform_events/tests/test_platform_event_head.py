"""Unit tests for PlatformEventsHead module.

Tests verify the core caching, deduplication, eviction, and REST API
logic inside the central PlatformEventsHead controller.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from ray._private import ray_constants
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.platform_event_pb2 import PlatformEvent, Source
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


def test_head_event_caching_and_update():
    head = _make_head()

    # Deliver a brand new RayEvent
    ray_event = RayEvent(
        event_id=b"test_uid",
        message="some message",
    )
    ray_event.platform_event.message = "some message"
    ray_event.platform_event.custom_fields["count"] = "1"

    head._process_event_callback(ray_event)
    assert len(head._events) == 1

    cached_event = next(iter(head._events.values()))
    assert cached_event.message == "some message"
    assert cached_event.platform_event.custom_fields["count"] == "1"

    # Deliver an updated RayEvent with same event_id
    updated_ray_event = RayEvent(
        event_id=b"test_uid",
        message="updated message",
    )
    updated_ray_event.platform_event.message = "updated message"
    updated_ray_event.platform_event.custom_fields["count"] = "5"

    head._process_event_callback(updated_ray_event)
    assert len(head._events) == 1

    cached_event_updated = next(iter(head._events.values()))
    assert cached_event_updated.message == "updated message"
    assert cached_event_updated.platform_event.custom_fields["count"] == "5"


def test_dedup_suppresses_repeated_uid():
    head = _make_head()
    ray_event = RayEvent(event_id=b"uid-repeated")

    head._process_event_callback(ray_event)
    head._process_event_callback(ray_event)

    assert len(head._events) == 1


def test_dedup_uid_eviction_keeps_recent():
    """Verify that cache size does not exceed MAX_EVENTS_TO_CACHE and keeps recent items."""
    head = _make_head()
    total = MAX_EVENTS_TO_CACHE + 10
    for i in range(total):
        ray_event = RayEvent(event_id=f"uid-{i}".encode())
        head._process_event_callback(ray_event)

    assert len(head._events) == MAX_EVENTS_TO_CACHE
    # Oldest UIDs should be gone
    assert "uid-0" not in head._events
    # Newest should still be there
    assert f"uid-{total - 1}" in head._events


def test_head_event_emission_via_event_recorder(monkeypatch):
    head = _make_head()
    ray_event = RayEvent(
        event_id=b"test_uid_emission",
        message="test emission message",
        severity=RayEvent.Severity.WARNING,
    )
    ray_event.platform_event.message = "test emission message"
    ray_event.platform_event.source.platform = Source.Platform.KUBERNETES
    ray_event.platform_event.source.component = "kubelet"
    ray_event.platform_event.source.metadata["namespace"] = "default"
    ray_event.platform_event.object_kind = "Pod"
    ray_event.platform_event.object_name = "test-pod"
    ray_event.platform_event.reason = "OOMKilled"
    ray_event.platform_event.custom_fields["count"] = "3"
    monkeypatch.setattr(
        ray_constants,
        "RAY_ENABLE_PYTHON_RAY_EVENT_TYPES",
        frozenset({"PLATFORM_EVENT"}),
    )
    captured = {}

    def fake_ray_event_ctor(**kwargs):
        captured.update(kwargs)
        return MagicMock(name="cython_ray_event")

    mock_recorder = MagicMock()
    with (
        patch(
            "ray._common.observability.internal_event.RayEvent",
            side_effect=fake_ray_event_ctor,
        ),
        patch.dict(
            sys.modules, {"ray._raylet": MagicMock(EventRecorder=mock_recorder)}
        ),
    ):
        head._process_event_callback(ray_event)

    mock_recorder.emit.assert_called_once()
    assert captured["entity_id"] == "test_uid_emission"
    assert captured["event_id"] == b"test_uid_emission"
    assert captured["source_type"] == RayEvent.SourceType.CLUSTER_LIFECYCLE
    assert captured["event_type"] == RayEvent.EventType.PLATFORM_EVENT
    assert captured["severity"] == RayEvent.Severity.WARNING
    assert captured["nested_event_field_number"] == (
        RayEvent.PLATFORM_EVENT_FIELD_NUMBER
    )

    nested = PlatformEvent()
    nested.ParseFromString(captured["serialized_data"])
    assert nested.source.platform == Source.Platform.KUBERNETES
    assert nested.source.component == "kubelet"
    assert nested.source.metadata["namespace"] == "default"
    assert nested.object_kind == "Pod"
    assert nested.object_name == "test-pod"
    assert nested.reason == "OOMKilled"
    assert nested.message == "test emission message"
    assert nested.custom_fields["count"] == "3"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
