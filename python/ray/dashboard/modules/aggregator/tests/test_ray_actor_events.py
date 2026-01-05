import base64
import json
import sys

import pytest

import ray
import ray.dashboard.consts as dashboard_consts
from ray._private import ray_constants
from ray._private.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.dashboard.tests.conftest import *  # noqa

_ACTOR_EVENT_PORT = 12346


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", _ACTOR_EVENT_PORT)


def wait_for_dashboard_agent_available(cluster):
    gcs_client = GcsClient(address=cluster.address)

    def get_dashboard_agent_address():
        return gcs_client.internal_kv_get(
            f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{cluster.head_node.node_id}".encode(),
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
            timeout=dashboard_consts.GCS_RPC_TIMEOUT_SECONDS,
        )

    wait_for_condition(lambda: get_dashboard_agent_address() is not None)


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

    def has_definition_and_alive_state():
        events = []
        for req, _ in httpserver.log:
            events.extend(json.loads(req.data))

        if not any("actorDefinitionEvent" in event for event in events):
            return False

        for event in events:
            if "actorLifecycleEvent" not in event:
                continue
            for state_transition in event["actorLifecycleEvent"]["stateTransitions"]:
                if state_transition["state"] == "ALIVE":
                    return True
        return False

    wait_for_condition(has_definition_and_alive_state)

    events = []
    for req, _ in httpserver.log:
        events.extend(json.loads(req.data))

    definition_events = [event for event in events if "actorDefinitionEvent" in event]
    assert definition_events
    definition_event = definition_events[0]
    assert (
        base64.b64decode(definition_event["actorDefinitionEvent"]["actorId"]).hex()
        == a._actor_id.hex()
    )
    assert base64.b64decode(definition_event["nodeId"]).hex() == head_node_id

    # Verify ActorId and state for ActorLifecycleEvents
    has_alive_state = False
    for actor_lifecycle_event in (
        event for event in events if "actorLifecycleEvent" in event
    ):
        assert base64.b64decode(actor_lifecycle_event["nodeId"]).hex() == head_node_id
        assert (
            base64.b64decode(
                actor_lifecycle_event["actorLifecycleEvent"]["actorId"]
            ).hex()
            == a._actor_id.hex()
        )
        for state_transition in actor_lifecycle_event["actorLifecycleEvent"][
            "stateTransitions"
        ]:
            assert state_transition["state"] in [
                "DEPENDENCIES_UNREADY",
                "PENDING_CREATION",
                "ALIVE",
                "RESTARTING",
                "DEAD",
            ]
            if state_transition["state"] == "ALIVE":
                has_alive_state = True
                assert (
                    base64.b64decode(state_transition["nodeId"]).hex() in all_nodes_ids
                )
                assert base64.b64decode(state_transition["workerId"]).hex() != ""
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
