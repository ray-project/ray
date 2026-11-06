import base64
import json
import os
import socket
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
        node_name="test-head-node",
        env_vars={
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": f"http://127.0.0.1:{_RAY_EVENT_PORT}",
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES": "NODE_DEFINITION_EVENT,NODE_LIFECYCLE_EVENT",
        },
        _system_config={
            "enable_ray_event": True,
        },
    )
    cluster.wait_for_nodes()
    head_node_id = cluster.head_node.node_id
    ray.init(address=cluster.address)
    wait_for_dashboard_agent_available(cluster)

    # Check that a node definition and a node lifecycle event are published.
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    wait_for_condition(lambda: len(httpserver.log) >= 1)
    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)
    assert len(req_json) == 2
    assert base64.b64decode(req_json[0]["nodeId"]).hex() == head_node_id
    assert (
        base64.b64decode(req_json[0]["nodeDefinitionEvent"]["nodeId"]).hex()
        == cluster.head_node.node_id
    )

    node_def_event = req_json[0]["nodeDefinitionEvent"]
    assert node_def_event["hostname"] == socket.gethostname()
    assert node_def_event["nodeName"] == "test-head-node"
    # instanceId and instanceTypeName are set via env vars by cloud providers.
    # In local/CI environments these are typically empty.
    assert node_def_event["instanceId"] == os.environ.get("RAY_CLOUD_INSTANCE_ID", "")
    assert node_def_event["instanceTypeName"] == os.environ.get(
        "RAY_CLOUD_INSTANCE_TYPE_NAME", ""
    )
    assert base64.b64decode(req_json[1]["nodeId"]).hex() == head_node_id
    assert (
        base64.b64decode(req_json[1]["nodeLifecycleEvent"]["nodeId"]).hex()
        == cluster.head_node.node_id
    )
    assert req_json[1]["nodeLifecycleEvent"]["stateTransitions"][0]["state"] == "ALIVE"
    assert (
        req_json[1]["nodeLifecycleEvent"]["stateTransitions"][0]["aliveSubState"]
        == "UNSPECIFIED"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
