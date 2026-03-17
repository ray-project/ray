import json
import logging
from collections import OrderedDict
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)

_EVENT_TYPE_TO_NESTED_KEY = {
    "AUTOSCALER_CONFIG_DEFINITION_EVENT": "autoscalerConfigDefinitionEvent",
    "AUTOSCALER_SCALING_DECISION_EVENT": "autoscalerScalingDecisionEvent",
    "AUTOSCALER_NODE_PROVISIONING_EVENT": "autoscalerNodeProvisioningEvent",
}


@dataclass(frozen=True)
class AutoscalerEventCacheInput:
    event_json: dict
    event_id: str
    timestamp_seconds: int
    size_bytes: int


class AutoscalerEventsStorage:
    def __init__(self, max_size_bytes: int):
        self._events: "OrderedDict[str, dict]" = OrderedDict()
        self._event_sizes: dict[str, int] = {}
        self._total_size_bytes = 0
        self._max_size_bytes = max_size_bytes

    def add_events(self, cache_inputs: List[AutoscalerEventCacheInput]) -> None:
        for cache_input in cache_inputs:
            self._add_event(cache_input)

    def get_events(self) -> "OrderedDict[str, dict]":
        return self._events

    def get_event_values(self) -> tuple[dict, ...]:
        return tuple(self._events.values())

    def _add_event(self, cache_input: AutoscalerEventCacheInput) -> None:
        event_id = cache_input.event_id
        size_bytes = cache_input.size_bytes

        if size_bytes > self._max_size_bytes:
            logger.warning(
                "Skipping autoscaler event %s because size %d exceeds cache budget %d.",
                event_id,
                size_bytes,
                self._max_size_bytes,
            )
            return

        dashboard_event = self._to_dashboard_event(cache_input)
        if dashboard_event is None:
            return

        # if the event is already in the cache, replace it with the new event
        if event_id in self._events:
            self._total_size_bytes -= self._event_sizes.pop(event_id)
            self._events.pop(event_id)

        self._events[event_id] = dashboard_event
        self._event_sizes[event_id] = size_bytes
        self._total_size_bytes += size_bytes

        # remove the oldest event if the total size exceeds the max size
        while self._total_size_bytes > self._max_size_bytes:
            oldest_event_id, _ = self._events.popitem(last=False)
            self._total_size_bytes -= self._event_sizes.pop(oldest_event_id)

    def _to_dashboard_event(
        self, cache_input: AutoscalerEventCacheInput
    ) -> Optional[dict]:
        event_json = cache_input.event_json
        nested_key = _EVENT_TYPE_TO_NESTED_KEY.get(event_json.get("eventType"))
        if nested_key is None or nested_key not in event_json:
            return None

        nested_payload = event_json[nested_key]
        return {
            "event_id": cache_input.event_id,
            "source_type": event_json["sourceType"],
            "message": json.dumps(nested_payload, sort_keys=True),
            "timestamp": cache_input.timestamp_seconds,
            "severity": event_json["severity"],
            "custom_fields": {},
        }
