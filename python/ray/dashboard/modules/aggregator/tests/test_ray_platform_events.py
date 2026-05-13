import base64
import json
import sys
import time

import pytest

import ray
from ray._private.test_utils import (
    wait_for_condition,
    wait_for_dashboard_agent_available,
)
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.dashboard.tests.conftest import *  # noqa

_PLATFORM_EVENT_PORT = 12348


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", _PLATFORM_EVENT_PORT)


def test_ray_platform_events(ray_start_cluster, httpserver):
    cluster = ray_start_cluster
    cluster.add_node(
        env_vars={
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": f"http://127.0.0.1:{_PLATFORM_EVENT_PORT}",
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES": "PLATFORM_EVENT",
            "RAY_ENABLE_PYTHON_RAY_EVENT_TYPES": "PLATFORM_EVENT",
        },
        _system_config={
            "enable_ray_event": True,
        },
    )
    cluster.wait_for_nodes()
    head_node_id = cluster.head_node.node_id

    ray.init(address=cluster.address)
    wait_for_dashboard_agent_available(cluster)

    # Define a task that explicitly initializes and emits a platform event via EventRecorder
    @ray.remote
    def emit_test_platform_event():
        from ray._common.observability.platform_events import PlatformEventBuilder
        from ray._raylet import EventRecorder
        from ray.core.generated.platform_event_pb2 import Source

        builder = PlatformEventBuilder(
            event_uid="uid-test-platform-e2e",
            platform=Source.Platform.KUBERNETES,
            object_kind="Pod",
            object_name="test-pod-name",
            reason="OOMKilled",
            message="Container exited with code 137",
            severity=RayEvent.Severity.WARNING,
            component="kubelet",
        )
        cython_event = builder.build(
            event_id=b"uid-test-platform-e2e",
            timestamp_ns=int(time.time() * 1e9),
        )
        EventRecorder.emit(cython_event)
        return True

    # Expect the POST request on the HTTP server
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    # Execute the remote task to emit the event on the node
    ray.get(emit_test_platform_event.remote())

    # Wait for the HTTP log collector to receive the batched payload
    wait_for_condition(lambda: len(httpserver.log) >= 1, timeout=20)

    # Validate the captured POST payload
    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)

    assert len(req_json) >= 1
    platform_event_entry = None
    for entry in req_json:
        if "platformEvent" in entry:
            platform_event_entry = entry
            break

    assert platform_event_entry is not None
    assert platform_event_entry["eventType"] == "PLATFORM_EVENT"
    assert base64.b64decode(platform_event_entry["nodeId"]).hex() == head_node_id

    pe_data = platform_event_entry["platformEvent"]
    assert pe_data["objectKind"] == "Pod"
    assert pe_data["objectName"] == "test-pod-name"
    assert pe_data["reason"] == "OOMKilled"
    assert pe_data["message"] == "Container exited with code 137"
    assert pe_data["source"]["platform"] == "KUBERNETES"
    assert pe_data["source"]["component"] == "kubelet"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
