import sys

import pytest

from ray._common.observability.health_events import HealthEventBuilder
from ray.core.generated.events_base_event_pb2 import RayEvent as RayEventProto
from ray.core.generated.events_health_event_pb2 import (
    HealthCheckerDisabled,
    HealthDecision,
    HealthEvent,
    HealthFinding,
)

Severity = RayEventProto.Severity


def _finding_event(state: int) -> HealthEvent:
    return HealthEvent(
        checker_name="nccl_hang_detector",
        sequence=1,
        finding=HealthFinding(
            finding_id="finding-1", state=state, summary="2 communicators stalled"
        ),
    )


def _disabled_event(reason: int) -> HealthEvent:
    return HealthEvent(
        checker_name="nccl_hang_detector",
        sequence=2,
        checker_disabled=HealthCheckerDisabled(reason=reason, detail="ncclras missing"),
    )


def _decision_event(action: int) -> HealthEvent:
    return HealthEvent(
        checker_name="health_manager",
        sequence=1,
        decision=HealthDecision(decision_id="decision-1", action=action, reason="hang"),
    )


@pytest.mark.parametrize(
    "event, entity_id, severity, message",
    [
        (
            _finding_event(HealthFinding.State.NEW),
            "finding-1",
            Severity.WARNING,
            "2 communicators stalled",
        ),
        (
            _finding_event(HealthFinding.State.UPDATED),
            "finding-1",
            Severity.WARNING,
            "2 communicators stalled",
        ),
        (
            _finding_event(HealthFinding.State.RESOLVED),
            "finding-1",
            Severity.INFO,
            "2 communicators stalled",
        ),
        (
            _finding_event(HealthFinding.State.STOPPED),
            "finding-1",
            Severity.WARNING,
            "2 communicators stalled",
        ),
        (
            _disabled_event(HealthCheckerDisabled.Reason.DEPENDENCY_UNAVAILABLE),
            "nccl_hang_detector",
            Severity.WARNING,
            "ncclras missing",
        ),
        (
            _disabled_event(HealthCheckerDisabled.Reason.INTERNAL_ERROR),
            "nccl_hang_detector",
            Severity.ERROR,
            "ncclras missing",
        ),
        (
            _decision_event(HealthDecision.Action.DIAGNOSE),
            "decision-1",
            Severity.INFO,
            "hang",
        ),
        (
            _decision_event(HealthDecision.Action.EVICT),
            "decision-1",
            Severity.WARNING,
            "hang",
        ),
    ],
)
def test_derived_fields(event, entity_id, severity, message):
    builder = HealthEventBuilder(event, session_name="session_x")

    assert builder.get_entity_id() == entity_id
    assert builder._severity == severity
    assert builder._message == message
    assert builder._source_type == RayEventProto.SourceType.RAY_TRAIN
    assert builder._event_type == RayEventProto.EventType.HEALTH_EVENT
    assert builder._nested_event_field_number == RayEventProto.HEALTH_EVENT_FIELD_NUMBER


def test_serialized_data_round_trips_through_ray_event():
    event = _finding_event(HealthFinding.State.NEW)
    builder = HealthEventBuilder(event)

    # The C++ recorder parses the bytes into RayEvent.health_event.
    ray_event = RayEventProto()
    ray_event.health_event.ParseFromString(builder.serialize_event_data())
    assert ray_event.health_event == event


def test_rejects_event_without_body():
    with pytest.raises(ValueError, match="no body"):
        HealthEventBuilder(HealthEvent(checker_name="x"))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
