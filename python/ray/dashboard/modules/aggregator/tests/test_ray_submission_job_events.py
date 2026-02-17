import base64
import json
import sys

import pytest

import ray
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_for_dashboard_agent_available,
    wait_until_server_available,
)
from ray.dashboard.tests.conftest import *  # noqa
from ray.job_submission import JobSubmissionClient

_RAY_EVENT_PORT = 12347


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", _RAY_EVENT_PORT)


def _collect_all_events(httpserver):
    """Collect all events from all HTTP batches received so far."""
    all_events = []
    for req, _ in httpserver.log:
        all_events.extend(json.loads(req.data))
    return all_events


def _get_lifecycle_states(all_events):
    """Extract the set of lifecycle state names from all events."""
    states = set()
    for e in all_events:
        if "submissionJobLifecycleEvent" in e:
            for st in e["submissionJobLifecycleEvent"]["stateTransitions"]:
                states.add(st["state"])
    return states


def test_ray_submission_job_events(ray_start_cluster, httpserver):
    cluster = ray_start_cluster
    cluster.add_node(
        env_vars={
            "RAY_enable_ray_event": "1",
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": f"http://127.0.0.1:{_RAY_EVENT_PORT}",
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES": "SUBMISSION_JOB_DEFINITION_EVENT,SUBMISSION_JOB_LIFECYCLE_EVENT",
        }
    )
    cluster.wait_for_nodes()
    head_node_id = cluster.head_node.node_id
    ray.init(address=cluster.address)
    wait_for_dashboard_agent_available(cluster)

    # Set up HTTP server to accept event exports before submitting the job.
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    # Submit a job via JobSubmissionClient.
    webui_url = cluster.webui_url
    assert wait_until_server_available(webui_url) is True
    client = JobSubmissionClient(format_web_url(webui_url))
    submission_id = client.submit_job(entrypoint="echo hello")

    # Wait for the job to reach a terminal state.
    wait_for_condition(
        lambda: client.get_job_status(submission_id).is_terminal(),
        timeout=30,
    )
    assert str(client.get_job_status(submission_id)) == "SUCCEEDED"

    # Wait for all expected lifecycle states to be exported.
    expected_states = {"PENDING", "RUNNING", "SUCCEEDED"}
    wait_for_condition(
        lambda: expected_states.issubset(
            _get_lifecycle_states(_collect_all_events(httpserver))
        ),
        timeout=30,
    )

    all_events = _collect_all_events(httpserver)

    # --- First event should be a definition event ---
    assert (
        "submissionJobDefinitionEvent" in all_events[0]
    ), f"Expected first event to be a definition event, got: {all_events[0]}"

    # --- Definition event ---
    def_events = [e for e in all_events if "submissionJobDefinitionEvent" in e]
    assert len(def_events) == 1, (
        f"Expected exactly 1 definition event, got {len(def_events)}. "
        f"All events: {all_events}"
    )
    def_event = def_events[0]
    assert def_event["eventType"] == "SUBMISSION_JOB_DEFINITION_EVENT"
    assert def_event["sourceType"] == "JOBS"
    assert base64.b64decode(def_event["nodeId"]).hex() == head_node_id

    def_data = def_event["submissionJobDefinitionEvent"]
    assert def_data["submissionId"] == submission_id
    assert def_data["entrypoint"] == "echo hello"

    # --- Lifecycle events ---
    lc_events = [e for e in all_events if "submissionJobLifecycleEvent" in e]
    assert len(lc_events) >= 3, (
        f"Expected at least 3 lifecycle events (PENDING, RUNNING, SUCCEEDED), "
        f"got {len(lc_events)}. All events: {all_events}"
    )

    # Build a map from state -> state transition for verification.
    state_transitions = {}
    for lc in lc_events:
        assert lc["eventType"] == "SUBMISSION_JOB_LIFECYCLE_EVENT"
        assert lc["sourceType"] == "JOBS"
        assert base64.b64decode(lc["nodeId"]).hex() == head_node_id

        lc_data = lc["submissionJobLifecycleEvent"]
        assert lc_data["submissionId"] == submission_id
        for st in lc_data["stateTransitions"]:
            state_transitions.setdefault(st["state"], []).append(st)

    # Verify PENDING transition.
    assert "PENDING" in state_transitions
    pending = state_transitions["PENDING"][0]
    assert pending["timestamp"] != ""

    # Verify RUNNING transition has driver info populated.
    assert "RUNNING" in state_transitions
    running = state_transitions["RUNNING"][0]
    assert running["timestamp"] != ""
    assert base64.b64decode(running["driverNodeId"]).hex() == head_node_id
    assert running["driverAgentHttpAddress"].startswith("http://")

    # Verify SUCCEEDED transition.
    assert "SUCCEEDED" in state_transitions
    succeeded = state_transitions["SUCCEEDED"][0]
    assert succeeded["timestamp"] != ""
    assert succeeded["driverExitCode"] == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
