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

_RAY_EVENT_PORT = 12345


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", _RAY_EVENT_PORT)


def test_ray_node_events(ray_start_cluster, httpserver):
    cluster = ray_start_cluster
    cluster.add_node(
        env_vars={
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": f"http://127.0.0.1:{_RAY_EVENT_PORT}",
            "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_HTTP_ENDPOINT_EXPOSABLE_EVENT_TYPES": "NODE_DEFINITION_EVENT,NODE_LIFECYCLE_EVENT",
        },
        _system_config={
            "enable_ray_event": True,
        },
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    wait_for_dashboard_agent_available(cluster)

    # Check that a node definition and a node lifecycle event are published.
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    # Wait until we observe both event types across requests.
    wait_for_condition(
        lambda: (
            any(
                "nodeDefinitionEvent" in item
                for req, _ in httpserver.log
                for item in json.loads(req.data)
            )
            and any(
                "nodeLifecycleEvent" in item
                for req, _ in httpserver.log
                for item in json.loads(req.data)
            )
        )
    )
    # Find the latest items for each event type across all requests.
    definition_event = next(
        item
        for req, _ in httpserver.log
        for item in json.loads(req.data)
        if "nodeDefinitionEvent" in item
    )
    lifecycle_event = next(
        item
        for req, _ in httpserver.log
        for item in json.loads(req.data)
        if "nodeLifecycleEvent" in item
    )
    assert (
        base64.b64decode(definition_event["nodeDefinitionEvent"]["nodeId"]).hex()
        == cluster.head_node.node_id
    )
    assert (
        base64.b64decode(lifecycle_event["nodeLifecycleEvent"]["nodeId"]).hex()
        == cluster.head_node.node_id
    )
    assert lifecycle_event["nodeLifecycleEvent"]["stateTransitions"][0]["state"] == "ALIVE"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
