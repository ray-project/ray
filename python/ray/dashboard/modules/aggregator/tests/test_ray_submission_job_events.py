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


def test_ray_submission_job_events(ray_start_cluster, httpserver):
    cluster = ray_start_cluster
    cluster.add_node(
        env_vars={
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": f"http://127.0.0.1:{_RAY_EVENT_PORT}",
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES": "SUBMISSION_JOB_DEFINITION_EVENT,SUBMISSION_JOB_LIFECYCLE_EVENT",
        },
        _system_config={
            "enable_ray_event": True,
        },
    )
    cluster.wait_for_nodes()
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

    # Wait for events to be exported to the HTTP endpoint.
    wait_for_condition(lambda: len(httpserver.log) >= 1, timeout=30)

    # Collect all events from all HTTP batches.
    all_events = []
    for req, _ in httpserver.log:
        all_events.extend(json.loads(req.data))

    # Verify at least one definition event with correct submission_id and entrypoint.
    def_events = [e for e in all_events if "submissionJobDefinitionEvent" in e]
    assert len(def_events) >= 1, (
        f"Expected at least 1 definition event, got {len(def_events)}. "
        f"All events: {all_events}"
    )
    def_event = def_events[0]["submissionJobDefinitionEvent"]
    assert def_event["submissionId"] == submission_id
    assert def_event["entrypoint"] == "echo hello"

    # Verify at least one lifecycle event with correct submission_id.
    lc_events = [e for e in all_events if "submissionJobLifecycleEvent" in e]
    assert len(lc_events) >= 1, (
        f"Expected at least 1 lifecycle event, got {len(lc_events)}. "
        f"All events: {all_events}"
    )
    for lc in lc_events:
        assert lc["submissionJobLifecycleEvent"]["submissionId"] == submission_id


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
