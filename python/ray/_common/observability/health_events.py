"""Event builder for health events.

This module provides the InternalEventBuilder subclass for emitting the health
plane's findings, checker shutdowns, and decisions via the Ray event export
framework.
"""

from ray._common.observability.internal_event import InternalEventBuilder
from ray.core.generated.events_base_event_pb2 import RayEvent as RayEventProto
from ray.core.generated.events_health_event_pb2 import (
    HealthCheckerDisabled,
    HealthDecision,
    HealthEvent,
    HealthFinding,
)
from ray.util.annotations import DeveloperAPI

Severity = RayEventProto.Severity


@DeveloperAPI
class HealthEventBuilder(InternalEventBuilder):
    """Builds a RayEvent wrapping a HealthEvent.

    The entity ID, severity, and message are derived from the event's body:

    - finding: the finding_id, WARNING (INFO once RESOLVED), and the summary.
    - checker_disabled: the checker_name, WARNING (ERROR for INTERNAL_ERROR),
      and the detail.
    - decision: the decision_id, INFO for DIAGNOSE (WARNING otherwise), and
      the reason.
    """

    def __init__(self, health_event: HealthEvent, session_name: str = ""):
        """Initialize the builder.

        Args:
            health_event: The HealthEvent to wrap. It must have a body set.
            session_name: The Ray session name.
        """
        body = health_event.WhichOneof("body")
        if body == "finding":
            finding = health_event.finding
            self._entity_id = finding.finding_id
            resolved = finding.state == HealthFinding.State.RESOLVED
            severity = Severity.INFO if resolved else Severity.WARNING
            message = finding.summary
        elif body == "checker_disabled":
            disabled = health_event.checker_disabled
            self._entity_id = health_event.checker_name
            internal_error = (
                disabled.reason == HealthCheckerDisabled.Reason.INTERNAL_ERROR
            )
            severity = Severity.ERROR if internal_error else Severity.WARNING
            message = disabled.detail
        elif body == "decision":
            decision = health_event.decision
            self._entity_id = decision.decision_id
            diagnose = decision.action == HealthDecision.Action.DIAGNOSE
            severity = Severity.INFO if diagnose else Severity.WARNING
            message = decision.reason
        else:
            raise ValueError("HealthEvent has no body set.")

        super().__init__(
            source_type=RayEventProto.SourceType.RAY_TRAIN,
            event_type=RayEventProto.EventType.HEALTH_EVENT,
            nested_event_field_number=RayEventProto.HEALTH_EVENT_FIELD_NUMBER,
            severity=severity,
            message=message,
            session_name=session_name,
        )
        self._health_event = health_event

    def get_entity_id(self) -> str:
        return self._entity_id

    def serialize_event_data(self) -> bytes:
        return self._health_event.SerializeToString()
