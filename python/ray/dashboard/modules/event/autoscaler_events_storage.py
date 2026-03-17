import logging
from collections import OrderedDict

from ray.autoscaler.v2.event_logger import build_autoscaler_scheduling_update_rows
from ray.core.generated import events_base_event_pb2

logger = logging.getLogger(__name__)

_SCALING_DECISION_EVENT = (
    events_base_event_pb2.RayEvent.EventType.AUTOSCALER_SCALING_DECISION_EVENT
)
_AUTOSCALER_SOURCE = events_base_event_pb2.RayEvent.SourceType.Name(
    events_base_event_pb2.RayEvent.SourceType.AUTOSCALER
)


class AutoscalerEventsStorage:
    def __init__(self, max_size_bytes: int):
        self._events: "OrderedDict[str, events_base_event_pb2.RayEvent]" = OrderedDict()
        self._event_sizes: dict[str, int] = {}
        self._total_size_bytes = 0
        self._max_size_bytes = max_size_bytes

    @staticmethod
    def is_supported_event(event: events_base_event_pb2.RayEvent) -> bool:
        return event.event_type == _SCALING_DECISION_EVENT

    def add_events(self, events: list[events_base_event_pb2.RayEvent]) -> None:
        for event in events:
            if not self.is_supported_event(event):
                continue
            self._add_event(event)

    def get_events(self) -> "OrderedDict[str, events_base_event_pb2.RayEvent]":
        return self._events

    def get_event_values(self) -> tuple[dict, ...]:
        return tuple(
            row
            for event in self._events.values()
            for row in self._render_event_rows(event)
        )

    def _add_event(self, event: events_base_event_pb2.RayEvent) -> None:
        event_id = event.event_id.hex()
        size_bytes = event.ByteSize()

        if size_bytes > self._max_size_bytes:
            logger.warning(
                "Skipping autoscaler event %s because size %d exceeds cache budget %d.",
                event_id,
                size_bytes,
                self._max_size_bytes,
            )
            return

        if event_id in self._events:
            self._total_size_bytes -= self._event_sizes.pop(event_id)
            self._events.pop(event_id)

        stored_event = events_base_event_pb2.RayEvent()
        stored_event.CopyFrom(event)
        self._events[event_id] = stored_event
        self._event_sizes[event_id] = size_bytes
        self._total_size_bytes += size_bytes

        while self._total_size_bytes > self._max_size_bytes:
            oldest_event_id, _ = self._events.popitem(last=False)
            self._total_size_bytes -= self._event_sizes.pop(oldest_event_id)

    def _render_event_rows(self, event: events_base_event_pb2.RayEvent) -> list[dict]:
        return [
            {
                "event_id": row["event_id"],
                "source_type": _AUTOSCALER_SOURCE,
                "message": row["message"],
                "timestamp": event.timestamp.seconds,
                "severity": row["severity"],
                "custom_fields": None,
            }
            for row in self._render_scaling_decision_rows(event)
        ]

    def _render_scaling_decision_rows(
        self, event: events_base_event_pb2.RayEvent
    ) -> list[dict]:
        scaling_event = event.autoscaler_scaling_decision_event
        rendered_rows = build_autoscaler_scheduling_update_rows(
            cluster_resources=dict(scaling_event.cluster_resources_after),
            launch_actions=[
                {
                    "instance_type": action.instance_type,
                    "count": action.count,
                }
                for action in scaling_event.launch_actions
            ],
            terminate_actions=[
                {
                    "cause": action.cause,
                    "instance_type": action.instance_type,
                    "count": action.count,
                }
                for action in scaling_event.terminate_actions
            ],
            infeasible_resource_requests=self._group_resource_bundles(
                scaling_event.infeasible_resource_requests
            ),
            infeasible_gang_resource_requests=[
                {
                    "details": gang_request.details,
                    "bundles": self._group_resource_bundles(gang_request.bundles),
                }
                for gang_request in scaling_event.infeasible_gang_resource_requests
            ],
            infeasible_cluster_resource_constraints=[
                {
                    "resource_requests": [
                        {
                            "request": self._resource_bundle_to_dict(
                                request_by_count.request
                            ),
                            "count": request_by_count.count,
                        }
                        for request_by_count in constraint.resource_requests
                    ]
                }
                for constraint in scaling_event.infeasible_cluster_resource_constraints
            ],
        )
        base_event_id = event.event_id.hex()
        return [
            {
                "event_id": f"{base_event_id}:{index}",
                "severity": row["severity"],
                "message": row["message"],
            }
            for index, row in enumerate(rendered_rows)
        ]

    def _group_resource_bundles(self, bundles) -> list[dict]:
        grouped: "OrderedDict[tuple, dict]" = OrderedDict()
        for bundle in bundles:
            bundle_dict = self._resource_bundle_to_dict(bundle)
            bundle_key = (
                tuple(sorted(bundle_dict["resources"].items())),
                tuple(
                    (
                        constraint["label_key"],
                        constraint["operator"],
                        tuple(constraint["values"]),
                    )
                    for constraint in bundle_dict["label_constraints"]
                ),
            )
            if bundle_key not in grouped:
                grouped[bundle_key] = {
                    "resources": bundle_dict["resources"],
                    "count": 0,
                }
                if bundle_dict["label_constraints"]:
                    grouped[bundle_key]["label_constraints"] = bundle_dict[
                        "label_constraints"
                    ]
            grouped[bundle_key]["count"] += 1
        return list(grouped.values())

    @staticmethod
    def _resource_bundle_to_dict(bundle) -> dict:
        return {
            "resources": dict(bundle.resources),
            "label_constraints": [
                {
                    "label_key": constraint.label_key,
                    "operator": constraint.operator,
                    "values": list(constraint.values),
                }
                for constraint in bundle.label_constraints
            ],
        }
