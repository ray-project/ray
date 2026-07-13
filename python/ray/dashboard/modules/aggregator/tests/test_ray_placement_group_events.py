import base64
import json
import sys

import pytest

import ray
from ray._private.test_utils import (
    wait_for_condition,
    wait_for_dashboard_agent_available,
)
from ray.dashboard.tests.conftest import *  # noqa

_PG_EVENT_PORT = 12347


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", _PG_EVENT_PORT)


def _get_all_events(httpserver):
    events = []
    for req, _ in httpserver.log:
        events.extend(json.loads(req.data))
    return events


def _get_state_transitions(httpserver, pg_id):
    transitions = []
    for event in _get_all_events(httpserver):
        pg_life = event.get("placementGroupLifecycleEvent")
        if pg_life is None:
            continue
        assert base64.b64decode(pg_life["placementGroupId"]).hex() == pg_id
        transitions.extend(pg_life["stateTransitions"])
    return transitions


def _has_state(httpserver, pg_id, state):
    return any(t["state"] == state for t in _get_state_transitions(httpserver, pg_id))


def test_ray_placement_group_events(ray_start_cluster, httpserver):
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    cluster = ray_start_cluster
    cluster.add_node(
        env_vars={
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": f"http://127.0.0.1:{_PG_EVENT_PORT}",
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES": "PLACEMENT_GROUP_DEFINITION_EVENT,PLACEMENT_GROUP_LIFECYCLE_EVENT",
        },
        _system_config={
            "enable_ray_event": True,
        },
    )
    cluster.wait_for_nodes()

    ray.init(address=cluster.address)
    wait_for_dashboard_agent_available(cluster)

    # Create a placement group to trigger definition + lifecycle events
    pg = ray.util.placement_group(
        name="test-pg",
        bundles=[{"CPU": 0.1}, {"CPU": 0.1}],
        strategy="SPREAD",
    )
    ray.get(pg.ready())
    pg_id = pg.id.hex()

    # The CREATED transition may arrive in a later batch than the definition
    # event, so collect events across all received batches.
    wait_for_condition(lambda: _has_state(httpserver, pg_id, "CREATED"))

    # Find and verify the definition event
    definition_event = None
    for event in _get_all_events(httpserver):
        if "placementGroupDefinitionEvent" in event:
            definition_event = event
            break

    assert definition_event is not None
    pg_def = definition_event["placementGroupDefinitionEvent"]
    assert base64.b64decode(pg_def["placementGroupId"]).hex() == pg_id
    assert pg_def["name"] == "test-pg"
    assert pg_def["strategy"] == "SPREAD"
    assert len(pg_def["bundles"]) == 2
    assert pg_def["bundles"][0]["bundleIndex"] == 0
    assert pg_def["bundles"][1]["bundleIndex"] == 1

    # Verify lifecycle state transitions
    for state_transition in _get_state_transitions(httpserver, pg_id):
        assert state_transition["state"] in [
            "PENDING",
            "PREPARED",
            "CREATED",
            "REMOVED",
            "RESCHEDULING",
        ]
        assert "timestamp" in state_transition
        if state_transition["state"] == "CREATED":
            # CREATED state should have FINISHED scheduling state
            assert state_transition["schedulingState"] == "FINISHED"

    # Remove the placement group and verify we get a REMOVED state
    ray.util.remove_placement_group(pg)

    wait_for_condition(lambda: _has_state(httpserver, pg_id, "REMOVED"))

    for state_transition in _get_state_transitions(httpserver, pg_id):
        if state_transition["state"] == "REMOVED":
            assert state_transition["schedulingState"] == "SCHEDULING_REMOVED"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
