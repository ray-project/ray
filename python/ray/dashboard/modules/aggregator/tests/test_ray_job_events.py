import pytest
import sys
import json
import base64

import ray
from ray._private.test_utils import (
    wait_until_server_available,
    wait_for_condition,
)
from ray.dashboard.tests.conftest import *  # noqa

from ray._private import ray_constants

_RAY_EVENT_PORT = 12345

@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", _RAY_EVENT_PORT)


def test_ray_job_events(ray_start_cluster, httpserver):
    cluster = ray_start_cluster
    cluster.add_node(
        env_vars={
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": f"http://127.0.0.1:{_RAY_EVENT_PORT}",
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES": "DRIVER_JOB_DEFINITION_EVENT,DRIVER_JOB_EXECUTION_EVENT",
            "RAY_enable_ray_event": "1",
        }
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Wait for the dashboard agent to be available
    ip, _ = cluster.webui_url.split(":")
    agent_address = f"{ip}:{ray_constants.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT}"
    assert wait_until_server_available(agent_address)

    # Submit a ray job
    @ray.remote
    def f():
        return 1

    ray.get(f.remote())

    # Check that a driver job event with the correct job id is published.
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    wait_for_condition(lambda: len(httpserver.log) >= 1)
    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)
    assert req_json[0]["message"] == "driver job definition event"
    assert (
        base64.b64decode(req_json[0]["driverJobDefinitionEvent"]["jobId"]).hex()
        == ray.get_runtime_context().get_job_id()
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
