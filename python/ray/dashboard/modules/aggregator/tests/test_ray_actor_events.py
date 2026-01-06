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

_ACTOR_EVENT_PORT = 12346


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", _ACTOR_EVENT_PORT)


def test_ray_actor_events(ray_start_cluster, httpserver):
    cluster = ray_start_cluster
    cluster.add_node(
        env_vars={
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": f"http://127.0.0.1:{_ACTOR_EVENT_PORT}",
            "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_HTTP_ENDPOINT_EXPOSABLE_EVENT_TYPES": "ACTOR_DEFINITION_EVENT,ACTOR_LIFECYCLE_EVENT",
        },
        _system_config={
            "enable_ray_event": True,
        },
    )
    cluster.wait_for_nodes()
    head_node_id = cluster.head_node.node_id
    all_nodes_ids = [node.node_id for node in cluster.list_all_nodes()]

    class A:
        def ping(self):
            return "pong"

    ray.init(address=cluster.address)
    wait_for_dashboard_agent_available(cluster)

    # Create an actor to trigger definition + lifecycle events
    a = ray.remote(A).options(name="actor-test").remote()
    ray.get(a.ping.remote())

    # Check that an actor definition and a lifecycle event are published.
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    wait_for_condition(lambda: len(httpserver.log) >= 1)
    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)
    # We expect batched events containing definition then lifecycle
    assert len(req_json) >= 2
    # Verify event types and IDs exist
    assert (
        base64.b64decode(req_json[0]["actorDefinitionEvent"]["actorId"]).hex()
        == a._actor_id.hex()
    )
    assert base64.b64decode(req_json[0]["nodeId"]).hex() == head_node_id
    # Verify ActorId and state for ActorLifecycleEvents
    has_alive_state = False
    for actorLifeCycleEvent in req_json[1:]:
        assert base64.b64decode(actorLifeCycleEvent["nodeId"]).hex() == head_node_id
        assert (
            base64.b64decode(
                actorLifeCycleEvent["actorLifecycleEvent"]["actorId"]
            ).hex()
            == a._actor_id.hex()
        )
        for stateTransition in actorLifeCycleEvent["actorLifecycleEvent"][
            "stateTransitions"
        ]:
            assert stateTransition["state"] in [
                "DEPENDENCIES_UNREADY",
                "PENDING_CREATION",
                "ALIVE",
                "RESTARTING",
                "DEAD",
            ]
            if stateTransition["state"] == "ALIVE":
                has_alive_state = True
                assert (
                    base64.b64decode(stateTransition["nodeId"]).hex() in all_nodes_ids
                )
                assert base64.b64decode(stateTransition["workerId"]).hex() != ""
    assert has_alive_state

    # Kill the actor and verify we get a DEAD state with death cause
    ray.kill(a)

    # Wait for the death event to be published
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    wait_for_condition(lambda: len(httpserver.log) >= 2)

    has_dead_state = False
    for death_req, _ in httpserver.log:
        death_req_json = json.loads(death_req.data)

        for actorLifeCycleEvent in death_req_json:
            if "actorLifecycleEvent" in actorLifeCycleEvent:
                assert (
                    base64.b64decode(
                        actorLifeCycleEvent["actorLifecycleEvent"]["actorId"]
                    ).hex()
                    == a._actor_id.hex()
                )

                for stateTransition in actorLifeCycleEvent["actorLifecycleEvent"][
                    "stateTransitions"
                ]:
                    if stateTransition["state"] == "DEAD":
                        has_dead_state = True
                        assert (
                            stateTransition["deathCause"]["actorDiedErrorContext"][
                                "reason"
                            ]
                            == "RAY_KILL"
                        )

    assert has_dead_state


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
