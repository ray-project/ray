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

_RAY_EVENT_PORT = 12345


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", _RAY_EVENT_PORT)


def wait_for_dashboard_agent_available(cluster):
    gcs_client = GcsClient(address=cluster.address)

    def get_dashboard_agent_address():
        return gcs_client.internal_kv_get(
            f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{cluster.head_node.node_id}".encode(),
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
            timeout=dashboard_consts.GCS_RPC_TIMEOUT_SECONDS,
        )

    wait_for_condition(lambda: get_dashboard_agent_address() is not None)


def test_ray_job_events(ray_start_cluster, httpserver):
    cluster = ray_start_cluster
    cluster.add_node(
        env_vars={
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": f"http://127.0.0.1:{_RAY_EVENT_PORT}",
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES": "DRIVER_JOB_DEFINITION_EVENT,DRIVER_JOB_EXECUTION_EVENT",
        },
        _system_config={
            "enable_ray_event": True,
        },
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    wait_for_dashboard_agent_available(cluster)

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
    assert (
        base64.b64decode(req_json[0]["driverJobDefinitionEvent"]["jobId"]).hex()
        == ray.get_runtime_context().get_job_id()
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
