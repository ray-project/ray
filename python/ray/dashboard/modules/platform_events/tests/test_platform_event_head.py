"""Unit tests for PlatformEventsHead module.

Tests verify the core caching, deduplication, eviction, and REST API
logic inside the central PlatformEventsHead controller.
"""

import pytest

from ray.core.generated.events_base_event_pb2 import RayEvent
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
