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


def test_ray_placement_group_events(ray_start_cluster, httpserver):
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

    # Check that a placement group definition and lifecycle events are published.
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    wait_for_condition(lambda: len(httpserver.log) >= 1)
    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)

    # We expect batched events containing definition then lifecycle
    assert len(req_json) >= 2

    # Find and verify the definition event
    definition_event = None
    for event in req_json:
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

    # Verify lifecycle events have the correct placement group ID and states
    has_created_state = False
    for event in req_json:
        if "placementGroupLifecycleEvent" in event:
            pg_life = event["placementGroupLifecycleEvent"]
            assert base64.b64decode(pg_life["placementGroupId"]).hex() == pg_id

            for state_transition in pg_life["stateTransitions"]:
                assert state_transition["state"] in [
                    "PENDING",
                    "PREPARED",
                    "CREATED",
                    "REMOVED",
                    "RESCHEDULING",
                ]
                assert "timestamp" in state_transition
                if state_transition["state"] == "CREATED":
                    has_created_state = True
                    # CREATED state should have FINISHED scheduling state
                    assert state_transition["schedulingState"] == "FINISHED"

    assert has_created_state

    # Remove the placement group and verify we get a REMOVED state
    ray.util.remove_placement_group(pg)

    # Wait for the removal event to be published
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    wait_for_condition(lambda: len(httpserver.log) >= 2)

    has_removed_state = False
    for removal_req, _ in httpserver.log:
        removal_req_json = json.loads(removal_req.data)

        for event in removal_req_json:
            if "placementGroupLifecycleEvent" in event:
                pg_life = event["placementGroupLifecycleEvent"]
                if base64.b64decode(pg_life["placementGroupId"]).hex() == pg_id:
                    for state_transition in pg_life["stateTransitions"]:
                        if state_transition["state"] == "REMOVED":
                            has_removed_state = True
                            assert (
                                state_transition["schedulingState"]
                                == "SCHEDULING_REMOVED"
                            )

    assert has_removed_state


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
