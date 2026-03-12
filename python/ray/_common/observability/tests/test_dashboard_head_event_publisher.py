from types import SimpleNamespace

import pytest

from ray._common.observability.dashboard_head_event_publisher import (
    DashboardHeadRayEventPublisher,
)
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.events_event_aggregator_service_pb2 import RayEventsData


class _FakeSession:
    def __init__(self):
        self.calls = []

    def post(self, url, json, headers, timeout):
        self.calls.append(
            {
                "url": url,
                "json": json,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return SimpleNamespace(ok=True)


def _build_serialized_events_data():
    events_data = RayEventsData(
        events=[
            RayEvent(
                event_id=b"1",
                source_type=RayEvent.SourceType.JOBS,
                event_type=RayEvent.EventType.DRIVER_JOB_LIFECYCLE_EVENT,
                severity=RayEvent.Severity.INFO,
                message="hello",
            )
        ]
    )
    return events_data.SerializeToString()


def test_dashboard_head_ray_event_publisher_posts_events(monkeypatch):
    session = _FakeSession()
    monkeypatch.setattr(
        "ray._common.observability.dashboard_head_event_publisher.serialize_events_to_ray_events_data",
        lambda events: _build_serialized_events_data(),
    )
    monkeypatch.setattr(
        "ray._common.observability.dashboard_head_event_publisher.get_auth_headers_if_auth_enabled",
        lambda headers: {},
    )

    publisher = DashboardHeadRayEventPublisher(
        dashboard_url="127.0.0.1:8265",
        headers={"X-Test": "1"},
        timeout_s=3,
        session=session,
    )
    publisher.publish_batch([object()])

    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"] == "http://127.0.0.1:8265/api/v0/external/ray_events"
    assert call["headers"] == {"X-Test": "1"}
    assert call["timeout"] == 3
    assert len(call["json"]) == 1
    assert call["json"][0]["eventType"] == "DRIVER_JOB_LIFECYCLE_EVENT"


def test_dashboard_head_ray_event_publisher_injects_auth_headers(monkeypatch):
    session = _FakeSession()
    monkeypatch.setattr(
        "ray._common.observability.dashboard_head_event_publisher.serialize_events_to_ray_events_data",
        lambda events: _build_serialized_events_data(),
    )
    monkeypatch.setattr(
        "ray._common.observability.dashboard_head_event_publisher.get_auth_headers_if_auth_enabled",
        lambda headers: {"Authorization": "Bearer loader-token"},
    )

    publisher = DashboardHeadRayEventPublisher(
        dashboard_url="http://127.0.0.1:8265",
        session=session,
    )
    publisher.publish_batch([object()])

    assert session.calls[0]["headers"]["Authorization"] == "Bearer loader-token"


def test_dashboard_head_ray_event_publisher_preserves_explicit_auth_header(monkeypatch):
    session = _FakeSession()
    seen_headers = {}

    def _get_auth_headers(headers):
        seen_headers.update(headers)
        return {}

    monkeypatch.setattr(
        "ray._common.observability.dashboard_head_event_publisher.serialize_events_to_ray_events_data",
        lambda events: _build_serialized_events_data(),
    )
    monkeypatch.setattr(
        "ray._common.observability.dashboard_head_event_publisher.get_auth_headers_if_auth_enabled",
        _get_auth_headers,
    )

    publisher = DashboardHeadRayEventPublisher(
        dashboard_url="http://127.0.0.1:8265",
        headers={"authorization": "Bearer user-token"},
        auth_token="should-not-be-used",
        session=session,
    )
    publisher.publish_batch([object()])

    assert seen_headers["authorization"] == "Bearer user-token"
    assert session.calls[0]["headers"]["authorization"] == "Bearer user-token"


def test_dashboard_head_ray_event_publisher_requires_target():
    with pytest.raises(ValueError):
        DashboardHeadRayEventPublisher()
