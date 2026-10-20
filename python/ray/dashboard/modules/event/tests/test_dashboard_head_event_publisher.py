import json
import logging
import sys

import pytest
import requests

from ray._common.observability.dashboard_head_event_publisher import (
    DashboardHeadRayEventPublisher,
)
from ray._raylet import RayEvent
from ray.core.generated.events_base_event_pb2 import RayEvent as RayEventProto
from ray.core.generated.events_driver_job_definition_event_pb2 import (
    DriverJobDefinitionEvent,
)


def _make_event(name="job-1"):
    nested = DriverJobDefinitionEvent()
    return RayEvent(
        source_type=RayEventProto.SourceType.JOBS,
        event_type=RayEventProto.EventType.DRIVER_JOB_DEFINITION_EVENT,
        severity=RayEventProto.Severity.INFO,
        entity_id=name,
        # entity_id is not part of the serialized proto; use message to
        # identify events in the posted payload.
        message=name,
        session_name="test-session",
        serialized_data=nested.SerializeToString(),
        nested_event_field_number=(
            RayEventProto.DRIVER_JOB_DEFINITION_EVENT_FIELD_NUMBER
        ),
    )


class _FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self.text = ""

    def raise_for_status(self):
        if not self.ok:
            raise requests.HTTPError(f"status {self.status_code}", response=self)


class _FakeSession:
    """Yields the configured responses/exceptions in order, then 200s."""

    def __init__(self, outcomes=None):
        self.outcomes = list(outcomes or [])
        self.requests = []

    def post(self, url, data=None, headers=None, timeout=None):
        self.requests.append((url, data))
        outcome = self.outcomes.pop(0) if self.outcomes else 200
        if isinstance(outcome, Exception):
            raise outcome
        return _FakeResponse(outcome)


def _make_publisher(session):
    return DashboardHeadRayEventPublisher(
        dashboard_url="localhost:8265", session=session
    )


def _sent_event_names(request):
    _, data = request
    return [e["message"] for e in json.loads(data)]


def test_publish_success_clears_buffer():
    session = _FakeSession()
    publisher = _make_publisher(session)
    publisher.publish(_make_event("a"))
    publisher.publish(_make_event("b"))
    assert len(session.requests) == 2
    # Second request must not resend the already-published event.
    assert _sent_event_names(session.requests[1]) == ["b"]


def test_connection_error_buffers_and_retries():
    session = _FakeSession([requests.ConnectionError("refused")])
    publisher = _make_publisher(session)
    # Does not raise; the event stays buffered.
    publisher.publish(_make_event("a"))
    publisher.publish(_make_event("b"))
    assert _sent_event_names(session.requests[1]) == ["a", "b"]


def test_server_error_buffers_and_retries():
    session = _FakeSession([500])
    publisher = _make_publisher(session)
    publisher.publish(_make_event("a"))
    publisher.publish(_make_event("b"))
    assert _sent_event_names(session.requests[1]) == ["a", "b"]


def test_client_error_drops_batch_and_raises():
    session = _FakeSession([422])
    publisher = _make_publisher(session)
    with pytest.raises(requests.HTTPError):
        publisher.publish(_make_event("a"))
    # Rejected batch is dropped, not retried.
    publisher.publish(_make_event("b"))
    assert _sent_event_names(session.requests[1]) == ["b"]


def test_buffer_overflow_drops_oldest_and_warns(monkeypatch, caplog):
    from ray._common.observability import dashboard_head_event_publisher as mod

    monkeypatch.setattr(mod, "_MAX_BUFFERED_EVENTS", 2)
    session = _FakeSession([requests.ConnectionError("refused")] * 3)
    publisher = _make_publisher(session)
    publisher.publish(_make_event("a"))
    publisher.publish(_make_event("b"))
    with caplog.at_level(logging.WARNING):
        publisher.publish(_make_event("c"))
    assert "dropping the 1 oldest" in caplog.text
    # The next successful publish sends only the retained (newest) events.
    publisher.publish_batch([])
    assert _sent_event_names(session.requests[-1]) == ["b", "c"]


def test_event_id_stable_across_retries():
    session = _FakeSession([requests.ConnectionError("refused")])
    publisher = _make_publisher(session)
    publisher.publish(_make_event("a"))
    publisher.publish_batch([])
    first = json.loads(session.requests[0][1])[0]["event_id"]
    retried = json.loads(session.requests[1][1])[0]["event_id"]
    assert first == retried


def test_dashboard_url_normalization():
    session = _FakeSession()
    for url, expected in [
        ("localhost:8265", "http://localhost:8265"),
        ("http://localhost:8265/", "http://localhost:8265"),
        ("https://host:1234", "https://host:1234"),
    ]:
        publisher = DashboardHeadRayEventPublisher(dashboard_url=url, session=session)
        publisher.publish(_make_event())
        assert session.requests[-1][0].startswith(
            expected + "/api/v0/external/ray_events"
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
