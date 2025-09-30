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
    all_nodes_ids = [node.node_id for node in cluster.list_all_nodes()]

    class A:
        def ping(self):
            return "pong"

    ray.init(address=cluster.address)
    wait_for_dashboard_agent_available(cluster)

    # Create an actor to trigger definition + lifecycle events
    a = ray.remote(A).options(name="actor-test").remote()
    ray.get(a.ping.remote())

    # Check that an actor definition and lifecycle events are published.
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    # Wait until we observe both event types across requests.
    wait_for_condition(
        lambda: (
            any(
                "actorDefinitionEvent" in item
                for req, _ in httpserver.log
                for item in json.loads(req.data)
            )
            and any(
                "actorLifecycleEvent" in item
                for req, _ in httpserver.log
                for item in json.loads(req.data)
            )
        )
    )
    # One definition event and potentially many lifecycle events across requests.
    definition_event = next(
        item
        for req, _ in httpserver.log
        for item in json.loads(req.data)
        if "actorDefinitionEvent" in item
    )
    lifecycle_events = [
        item
        for req, _ in httpserver.log
        for item in json.loads(req.data)
        if "actorLifecycleEvent" in item
    ]
    # Verify IDs
    assert (
        base64.b64decode(definition_event["actorDefinitionEvent"]["actorId"]).hex()
        == a._actor_id.hex()
    )
    # Verify lifecycle events
    has_alive_state = False
    for lifecycle_event in lifecycle_events:
        assert (
            base64.b64decode(lifecycle_event["actorLifecycleEvent"]["actorId"]).hex()
            == a._actor_id.hex()
        )
        for stateTransition in lifecycle_event["actorLifecycleEvent"][
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

    # Wait until a lifecycle event with DEAD state is observed in any request.
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    wait_for_condition(
        lambda: any(
            any(
                st.get("state") == "DEAD"
                for st in item["actorLifecycleEvent"].get("stateTransitions", [])
            )
            for req, _ in httpserver.log
            for item in json.loads(req.data)
            if "actorLifecycleEvent" in item
        )
    )

    has_dead_state = False
    for death_req, _ in httpserver.log:
        death_req_json = json.loads(death_req.data)
        for lifecycle_event in death_req_json:
            if "actorLifecycleEvent" in lifecycle_event:
                assert (
                    base64.b64decode(
                        lifecycle_event["actorLifecycleEvent"]["actorId"]
                    ).hex()
                    == a._actor_id.hex()
                )
                for stateTransition in lifecycle_event["actorLifecycleEvent"][
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
