from typing import List, Optional, Dict, Any, Union


def filter_events(
    events: List[Dict[str, Union[str, Dict]]],
    severity_levels: List[str],
    source_types: List[str],
    entity_name: Optional[str] = None,
    entity_id: Optional[str] = None,
) -> List[Dict[str, Union[str, Dict]]]:
    """
    Filter a list of events based on given criteria.

    Args:
    - events (List[Dict[str, Any]]): The list of events to be filtered.
    - severity_levels (List[str]): List of accepted severity levels for filtering.
    - source_types (List[str]): List of accepted source types for filtering.
    - entity_name (Optional[str]): Name of the entity to be matched in event's
    custom fields. e.g. "serve_app_name", "job_id"
    - entity_id (Optional[str]): ID of the entity to be matched in event's
    custom fields. It could be a string to represent a specific entity, like id for
    "job_id" or name for "serve_app_name We could also accept "*" to represent we
    will fetch all entities with the given entity_name no matter what the
    entity_id is.

    Returns:
    - List[Dict[str, Union[str, Dict]]]: A filtered list of events.
    """

    def event_filter(event: Dict[str, Any]) -> bool:
        # Filter 1: severity_level and source_type
        if severity_levels and event["severity"] not in severity_levels:
            return False
        if source_types and event["source_type"] not in source_types:
            return False

        # Filter 2: {entityName: entityName} (if provided)
        # should exist in custom_fields
        if entity_name and entity_id:
            custom_fields = event.get("custom_fields", {})
            # if entity_id = "*", we will return all events that have entity_name
            # existed in custom_fields whatever the entity_id is.
            # for example, entity_name="serve_app_name" and entity_id="*"
            # we will return all events that have "serve_app_name" in their
            # custom_fields
            if entity_id == "*":
                if entity_name not in custom_fields:
                    return False
            elif custom_fields.get(entity_name) != entity_id:
                return False

        return True

    return list(filter(event_filter, events))
