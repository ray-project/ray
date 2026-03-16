"""E2E tests for autoscaler ONE-event emission.

Starts an ``AutoscalingCluster`` with the V2 autoscaler and verifies that
autoscaler events are emitted through the ONE-event pipeline
(dashboard head -> local aggregator agent -> HTTP endpoint).

Scenarios tested:
  1. Config definition event emitted on autoscaler startup.
  2. Scaling-decision event with launch actions when a task demands more CPUs
     than the head node provides.
  3. Scaling-decision event with infeasible resource requests when a task
     requires a resource that no node type can provide.
  4. Config definition event re-emitted when the config file changes.
  5. Node provisioning event emitted when instances are requested.
"""

import json
import sys
import time
from typing import Callable, List

import pytest
from pytest_httpserver import HTTPServer

import ray
from ray._common.test_utils import wait_for_condition
from ray.cluster_utils import AutoscalingCluster

_RAY_EVENT_PORT = 12346

_AUTOSCALER_EVENT_TYPES = (
    "AUTOSCALER_CONFIG_DEFINITION_EVENT,"
    "AUTOSCALER_SCALING_DECISION_EVENT,"
    "AUTOSCALER_NODE_PROVISIONING_EVENT"
)

# Map UPPER_SNAKE event type names to the camelCase nested-dict key used in
# the protobuf JSON serialization (e.g. "autoscalerConfigDefinitionEvent").
_EVENT_TYPE_TO_EVENT_KEY_MAP = {
    "AUTOSCALER_CONFIG_DEFINITION_EVENT": "autoscalerConfigDefinitionEvent",
    "AUTOSCALER_SCALING_DECISION_EVENT": "autoscalerScalingDecisionEvent",
    "AUTOSCALER_NODE_PROVISIONING_EVENT": "autoscalerNodeProvisioningEvent",
}


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", _RAY_EVENT_PORT)


def _collect_events(httpserver) -> List[dict]:
    """Parse all events received by the HTTP server so far."""
    events = []
    for req, _ in httpserver.log:
        events.extend(json.loads(req.data))
    return events


def _get_events_of_type(httpserver, event_type: str) -> List[dict]:
    """Return all events matching a given eventType string."""
    return [e for e in _collect_events(httpserver) if e.get("eventType") == event_type]


def _wait_for_event(
    httpserver: HTTPServer,
    event_type: str,
    predicate: Callable[[dict], bool],
    timeout: int = 20,
    offset: int = 0,
) -> dict:
    """Poll until an event of *event_type* satisfies *predicate*, then return it.

    Args:
        httpserver: The pytest-httpserver fixture.
        event_type: The ``eventType`` string to filter on.
        predicate: Called with the **nested** event dict (e.g. the value of
            ``autoscalerConfigDefinitionEvent``).  Return ``True`` to accept.
        timeout: Maximum seconds to wait.
        offset: Skip events before this index (useful for waiting on *new*
            events after a known count).

    Returns:
        The full event envelope dict that matched.

    Raises:
        RuntimeError: If no matching event arrives within *timeout*.
    """
    nested_key = _EVENT_TYPE_TO_EVENT_KEY_MAP[event_type]
    result = {}  # container to capture the matched event

    def _condition():
        events = _get_events_of_type(httpserver, event_type)
        for evt in events[offset:]:
            if predicate(evt.get(nested_key, {})):
                result["event"] = evt
                return True
        return False

    wait_for_condition(_condition, timeout=timeout)
    return result["event"]


def _assert_common_fields(event: dict, expected_event_type: str) -> None:
    """Validate the common fields present in every exported event."""
    assert (
        event["sourceType"] == "AUTOSCALER"
    ), f"Expected sourceType AUTOSCALER, got {event.get('sourceType')}"
    assert event["eventType"] == expected_event_type
    assert event.get("eventId"), "eventId must be non-empty"


@pytest.fixture
def autoscaling_cluster(httpserver):
    """Start an AutoscalingCluster with ONE-event enabled and yield it."""
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 1},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
        autoscaler_v2=True,
    )

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    cluster.start(
        override_env={
            "RAY_enable_python_ray_event": "true",
            "RAY_external_ray_event_allowlist": _AUTOSCALER_EVENT_TYPES,
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": (
                f"http://127.0.0.1:{_RAY_EVENT_PORT}"
            ),
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES": (
                _AUTOSCALER_EVENT_TYPES
            ),
        },
    )
    ray.init("auto")

    yield cluster

    ray.shutdown()
    cluster.shutdown()


def test_config_definition_event(autoscaling_cluster, httpserver):
    """The autoscaler emits a config definition event at startup with the
    correct autoscaler version, provider, node types, and resource info."""

    event = _wait_for_event(
        httpserver,
        "AUTOSCALER_CONFIG_DEFINITION_EVENT",
        predicate=lambda n: n.get("autoscalerVersion") == "V2",
    )

    _assert_common_fields(event, "AUTOSCALER_CONFIG_DEFINITION_EVENT")

    nested = event["autoscalerConfigDefinitionEvent"]

    # Autoscaler-level fields.
    assert nested["autoscalerVersion"] == "V2"
    assert nested.get("cloudProviderType"), "cloudProviderType must be set"
    assert isinstance(nested.get("maxWorkers"), int)
    assert nested["maxWorkers"] >= 0

    # Validate available node types.
    node_types = nested.get("availableNodeTypes", [])
    assert (
        len(node_types) >= 2
    ), f"Expected at least 2 node types (head + type-1), got {len(node_types)}"

    type_by_name = {nt["nodeTypeName"]: nt for nt in node_types}

    # Validate the worker node type "type-1".
    assert (
        "type-1" in type_by_name
    ), f"Expected 'type-1' in node types, got {list(type_by_name.keys())}"
    type_1 = type_by_name["type-1"]
    assert type_1["maxWorkerNodes"] == 2
    assert type_1["minWorkerNodes"] == 0
    assert type_1.get("resources", {}).get("CPU") == 1.0

    # Validate the head node type exists.
    assert (
        "ray.head.default" in type_by_name
    ), f"Expected 'ray.head.default' in node types, got {list(type_by_name.keys())}"


def test_scaling_decision_event(autoscaling_cluster, httpserver):
    """Submit a task that forces the autoscaler to launch a worker and verify
    the scaling-decision event contains valid launch actions and cluster
    resource information."""

    @ray.remote(num_cpus=1)
    def f():
        time.sleep(999)

    obj_ref = f.remote()

    event = _wait_for_event(
        httpserver,
        "AUTOSCALER_SCALING_DECISION_EVENT",
        predicate=lambda n: bool(n.get("launchActions")),
    )
    ray.cancel(obj_ref, force=True)

    _assert_common_fields(event, "AUTOSCALER_SCALING_DECISION_EVENT")

    nested = event["autoscalerScalingDecisionEvent"]

    # Validate launch actions.
    launch_actions = nested["launchActions"]
    assert len(launch_actions) >= 1
    for action in launch_actions:
        assert action.get("instanceType"), "instanceType must be non-empty"
        assert isinstance(action.get("count"), int)
        assert action["count"] > 0

    # clusterResourcesAfter should be present (may be empty if head has 0 CPUs).
    assert "clusterResourcesAfter" in nested


def test_infeasible_resource_event(autoscaling_cluster, httpserver):
    """Request a resource that no node type can provide and verify the
    scaling-decision event reports it as infeasible with GPU in the
    resource bundle."""

    # Request GPUs -- no node type in the config has GPUs.
    @ray.remote(num_gpus=1)
    def g():
        time.sleep(999)

    obj_ref = g.remote()

    event = _wait_for_event(
        httpserver,
        "AUTOSCALER_SCALING_DECISION_EVENT",
        predicate=lambda n: bool(n.get("infeasibleResourceRequests")),
    )
    ray.cancel(obj_ref, force=True)

    _assert_common_fields(event, "AUTOSCALER_SCALING_DECISION_EVENT")

    nested = event["autoscalerScalingDecisionEvent"]

    # Validate infeasible resource requests contain a GPU demand.
    infeasible = nested["infeasibleResourceRequests"]
    assert len(infeasible) >= 1
    gpu_found = any("GPU" in req.get("resources", {}) for req in infeasible)
    assert (
        gpu_found
    ), f"Expected at least one infeasible request with GPU, got {infeasible}"


def test_config_change_emits_new_event(autoscaling_cluster, httpserver):
    """Modify the autoscaling config file after cluster startup and verify
    that a new config definition event is emitted with the updated value."""

    # Wait for the initial config definition event.
    _wait_for_event(
        httpserver,
        "AUTOSCALER_CONFIG_DEFINITION_EVENT",
        predicate=lambda n: n.get("autoscalerVersion") == "V2",
    )

    initial_count = len(
        _get_events_of_type(httpserver, "AUTOSCALER_CONFIG_DEFINITION_EVENT")
    )

    # Modify the config: change max_workers from 2 to 5 for type-1.
    autoscaling_cluster.update_config(
        {"available_node_types": {"type-1": {"max_workers": 5}}}
    )

    # Wait for a new config definition event with the updated value.
    def _has_updated_type1(nested: dict) -> bool:
        for nt in nested.get("availableNodeTypes", []):
            if nt.get("nodeTypeName") == "type-1" and nt.get("maxWorkerNodes") == 5:
                return True
        return False

    event = _wait_for_event(
        httpserver,
        "AUTOSCALER_CONFIG_DEFINITION_EVENT",
        predicate=_has_updated_type1,
        offset=initial_count,
    )

    _assert_common_fields(event, "AUTOSCALER_CONFIG_DEFINITION_EVENT")

    nested = event["autoscalerConfigDefinitionEvent"]

    # The updated event should still carry the full config.
    assert nested["autoscalerVersion"] == "V2"
    type_by_name = {
        nt["nodeTypeName"]: nt for nt in nested.get("availableNodeTypes", [])
    }
    assert type_by_name["type-1"]["maxWorkerNodes"] == 5
    assert "ray.head.default" in type_by_name


def test_node_provisioning_event(autoscaling_cluster, httpserver):
    """Submit a task that forces the autoscaler to request new instances and
    verify a node provisioning event is emitted with requested instances."""

    @ray.remote(num_cpus=1)
    def f():
        time.sleep(999)

    obj_ref = f.remote()

    event = _wait_for_event(
        httpserver,
        "AUTOSCALER_NODE_PROVISIONING_EVENT",
        predicate=lambda n: bool(n.get("requestedInstances")),
    )
    ray.cancel(obj_ref, force=True)

    _assert_common_fields(event, "AUTOSCALER_NODE_PROVISIONING_EVENT")

    nested = event["autoscalerNodeProvisioningEvent"]

    # Validate requested instances.
    requested = nested["requestedInstances"]
    assert len(requested) >= 1
    for inst in requested:
        assert inst.get("rayNodeTypeName"), "rayNodeTypeName must be non-empty"
        assert isinstance(inst.get("count"), int)
        assert inst["count"] > 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
