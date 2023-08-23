import logging
import pytest
import async_timeout
from ray.dashboard.modules.state.state_head import filter_events

logger = logging.getLogger(__name__)


async def http_get(http_session, url, timeout_seconds=60):
    with async_timeout.timeout(timeout_seconds):
        async with http_session.get(url) as response:
            return await response.json()


events = [
    {"severity": "ERROR", "source_type": "GCS", "custom_fields": {"job_id": 1}},
    {
        "severity": "DEBUG",
        "source_type": "CORE_WORKER",
        "custom_fields": {"serve_app_name": 42},
    },
    {
        "severity": "WARNING",
        "source_type": "GCS",
        "custom_fields": {"job_id": 2, "serve_replica_id": 10},
    },
    {
        "severity": "INFO",
        "source_type": "RAYLET",
        "custom_fields": {"serve_replica_id": 3},
    },
]


@pytest.mark.parametrize(
    "severity_levels, source_types, entity_name, entity_id, expected_output",
    [
        (["ERROR"], None, None, None, [events[0]]),
        (None, ["GCS"], None, None, [events[0], events[2]]),
        (None, None, "job_id", "1", [events[0]]),
        (None, None, "serve_app_name", "42", [events[1]]),
        (None, None, "serve_replica_id", "10", [events[2]]),
        (["WARNING"], ["GCS"], "job_id", "2", [events[2]]),
        (None, None, "serve_replica_id", "*", [events[2], events[3]]),
    ],
)
def test_filter_events(
    severity_levels, source_types, entity_name, entity_id, expected_output
):
    result = filter_events(
        events, severity_levels, source_types, entity_name, entity_id
    )
    assert result == expected_output
