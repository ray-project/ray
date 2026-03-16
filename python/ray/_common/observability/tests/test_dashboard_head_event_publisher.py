import json
import sys
from types import SimpleNamespace

import pytest

from ray._common.observability.dashboard_head_event_publisher import (
    DashboardHeadRayEventPublisher,
)
from ray._private.authentication_test_utils import (
    authentication_env_guard,
    reset_auth_token_state,
    set_auth_mode,
    set_env_auth_token,
)
from ray._raylet import RayEvent as CythonRayEvent
from ray.core.generated.events_base_event_pb2 import RayEvent as RayEventProto
from ray.core.generated.events_driver_job_lifecycle_event_pb2 import (
    DriverJobLifecycleEvent,
)


class _FakeSession:
    def __init__(self):
        self.calls = []

    def post(self, url, data, headers, timeout):
        self.calls.append(
            {
                "url": url,
                "data": data,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return SimpleNamespace(ok=True)


def _make_test_event(entity_id="test-entity"):
    """Create a real RayEvent for testing."""
    nested = DriverJobLifecycleEvent(job_id=entity_id.encode())
    return CythonRayEvent(
        source_type=RayEventProto.SourceType.JOBS,
        event_type=RayEventProto.EventType.DRIVER_JOB_LIFECYCLE_EVENT,
        severity=RayEventProto.Severity.INFO,
        entity_id=entity_id,
        message="test event",
        session_name="test-session",
        serialized_data=nested.SerializeToString(),
        nested_event_field_number=RayEventProto.DRIVER_JOB_LIFECYCLE_EVENT_FIELD_NUMBER,
    )


def test_dashboard_head_ray_event_publisher_posts_events():
    session = _FakeSession()
    publisher = DashboardHeadRayEventPublisher(
        dashboard_url="127.0.0.1:8265",
        headers={"X-Test": "1"},
        timeout_s=3,
        session=session,
    )
    publisher.publish(_make_test_event())

    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"] == "http://127.0.0.1:8265/api/v0/external/ray_events"
    assert call["headers"] == {"X-Test": "1", "Content-Type": "application/json"}
    assert call["timeout"] == 3
    payload = json.loads(call["data"])
    assert len(payload) == 1
    event_dict = payload[0]
    assert event_dict["eventType"] == "DRIVER_JOB_LIFECYCLE_EVENT"
    assert event_dict["sourceType"] == "JOBS"
    assert "driverJobLifecycleEvent" in event_dict


def test_dashboard_head_ray_event_publisher_injects_auth_headers():
    with authentication_env_guard():
        set_auth_mode("token")
        set_env_auth_token("loader-token-pad-to-pass-validation-checks-1234567")
        reset_auth_token_state()

        session = _FakeSession()
        publisher = DashboardHeadRayEventPublisher(
            dashboard_url="http://127.0.0.1:8265",
            session=session,
        )
        publisher.publish(_make_test_event())

        call_headers = session.calls[0]["headers"]
        assert call_headers.get("authorization") == (
            "Bearer loader-token-pad-to-pass-validation-checks-1234567"
        )


def test_dashboard_head_ray_event_publisher_preserves_explicit_auth_header():
    with authentication_env_guard():
        set_auth_mode("token")
        set_env_auth_token("loader-token-pad-to-pass-validation-checks-1234567")
        reset_auth_token_state()

        session = _FakeSession()
        publisher = DashboardHeadRayEventPublisher(
            dashboard_url="http://127.0.0.1:8265",
            headers={"authorization": "Bearer user-token"},
            auth_token="should-not-be-used",
            session=session,
        )
        publisher.publish(_make_test_event())

        assert session.calls[0]["headers"]["authorization"] == "Bearer user-token"


def test_dashboard_head_ray_event_publisher_requires_target():
    with pytest.raises(ValueError):
        DashboardHeadRayEventPublisher()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
