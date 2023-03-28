import os
import sys
import json
import jsonschema
import hashlib

import pprint
import pytest
import requests

import ray
from ray import serve
from ray.serve._private.constants import SERVE_NAMESPACE
from ray._private.test_utils import (
    format_web_url,
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    wait_for_condition,
)
from ray.dashboard import dashboard
from ray.dashboard.consts import RAY_CLUSTER_ACTIVITY_HOOK
from ray.dashboard.modules.snapshot.snapshot_head import RayActivityResponse
from ray.dashboard.tests.conftest import *  # noqa


@pytest.fixture
def set_ray_cluster_activity_hook(request):
    """
    Fixture that sets RAY_CLUSTER_ACTIVITY_HOOK environment variable
    for test_e2e_component_activities_hook.
    """
    external_hook = getattr(request, "param")
    assert (
        external_hook
    ), "Please pass value of RAY_CLUSTER_ACTIVITY_HOOK env var to this fixture"
    old_hook = os.environ.get(RAY_CLUSTER_ACTIVITY_HOOK)
    os.environ[RAY_CLUSTER_ACTIVITY_HOOK] = external_hook

    yield external_hook

    if old_hook is not None:
        os.environ[RAY_CLUSTER_ACTIVITY_HOOK] = old_hook
    else:
        del os.environ[RAY_CLUSTER_ACTIVITY_HOOK]


@pytest.mark.parametrize(
    "set_ray_cluster_activity_hook",
    [
        "ray._private.test_utils.external_ray_cluster_activity_hook1",
        "ray._private.test_utils.external_ray_cluster_activity_hook2",
        "ray._private.test_utils.external_ray_cluster_activity_hook3",
        "ray._private.test_utils.external_ray_cluster_activity_hook4",
        "ray._private.test_utils.external_ray_cluster_activity_hook5",
    ],
    indirect=True,
)
def test_component_activities_hook(set_ray_cluster_activity_hook, call_ray_start):
    """
    Tests /api/component_activities returns correctly for various
    responses of RAY_CLUSTER_ACTIVITY_HOOK.

    Verify no active drivers are correctly reflected in response.
    """
    external_hook = set_ray_cluster_activity_hook

    response = requests.get("http://127.0.0.1:8265/api/component_activities")
    response.raise_for_status()

    # Validate schema of response
    data = response.json()
    schema_path = os.path.join(
        os.path.dirname(dashboard.__file__),
        "modules/snapshot/component_activities_schema.json",
    )
    pprint.pprint(data)
    jsonschema.validate(instance=data, schema=json.load(open(schema_path)))

    # Validate driver response can be cast to RayActivityResponse object
    # and that there are no active drivers.
    driver_ray_activity_response = RayActivityResponse(**data["driver"])
    assert driver_ray_activity_response.is_active == "INACTIVE"
    assert driver_ray_activity_response.reason is None

    # Validate external component response can be cast to RayActivityResponse object
    if external_hook[-1] == "5":
        external_activity_response = RayActivityResponse(**data["test_component5"])
        assert external_activity_response.is_active == "ACTIVE"
        assert external_activity_response.reason == "Counter: 1"
    elif external_hook[-1] == "4":
        external_activity_response = RayActivityResponse(**data["external_component"])
        assert external_activity_response.is_active == "ERROR"
        assert (
            "'Error in external cluster activity hook'"
            in external_activity_response.reason
        )
    elif external_hook[-1] == "3":
        external_activity_response = RayActivityResponse(**data["external_component"])
        assert external_activity_response.is_active == "ERROR"
    elif external_hook[-1] == "2":
        external_activity_response = RayActivityResponse(**data["test_component2"])
        assert external_activity_response.is_active == "ERROR"
    elif external_hook[-1] == "1":
        external_activity_response = RayActivityResponse(**data["test_component1"])
        assert external_activity_response.is_active == "ACTIVE"
        assert external_activity_response.reason == "Counter: 1"

        # Call endpoint again to validate different response
        response = requests.get("http://127.0.0.1:8265/api/component_activities")
        response.raise_for_status()
        data = response.json()
        jsonschema.validate(instance=data, schema=json.load(open(schema_path)))

        external_activity_response = RayActivityResponse(**data["test_component1"])
        assert external_activity_response.is_active == "ACTIVE"
        assert external_activity_response.reason == "Counter: 2"


def test_active_component_activities(ray_start_with_dashboard):
    # Verify drivers which don't have namespace starting with _ray_internal_
    # are considered active.

    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    driver_template = """
import ray

ray.init(address="auto", namespace="{namespace}")
import time
time.sleep({sleep_time_s})
    """
    run_string_as_driver_nonblocking(
        driver_template.format(namespace="my_namespace", sleep_time_s=0)
    )

    def jobs_over():
        jobs_snapshot_data = requests.get(f"{webui_url}/api/snapshot").json()["data"][
            "snapshot"
        ]["jobs"]
        for job_id, job in jobs_snapshot_data.items():
            if job.get("isDead", None) is True:
                return True
        return False

    # Wait for above driver to start and finish
    wait_for_condition(jobs_over)

    run_string_as_driver_nonblocking(
        driver_template.format(namespace="my_namespace", sleep_time_s=5)
    )
    run_string_as_driver_nonblocking(
        driver_template.format(namespace="_ray_internal_job_info_id1", sleep_time_s=5)
    )
    # Simulate the default driver that gets created by dashboard
    run_string_as_driver_nonblocking(
        driver_template.format(namespace="_ray_internal_dashboard", sleep_time_s=5)
    )

    def jobs_started():
        jobs_snapshot_data = requests.get(f"{webui_url}/api/snapshot").json()["data"][
            "snapshot"
        ]["jobs"]
        job_names = {
            job["config"].get("namespace", "")
            for job_id, job in jobs_snapshot_data.items()
            if job.get("config") is not None
        }
        assert job_names.issuperset(
            {
                "my_namespace",
                "_ray_internal_dashboard",
                "_ray_internal_job_info_id1",
            }
        )
        return True

    wait_for_condition(jobs_started)

    # Verify drivers are considered active after running script
    response = requests.get(f"{webui_url}/api/component_activities")
    response.raise_for_status()

    # Validate schema of response
    data = response.json()
    schema_path = os.path.join(
        os.path.dirname(dashboard.__file__),
        "modules/snapshot/component_activities_schema.json",
    )

    jsonschema.validate(instance=data, schema=json.load(open(schema_path)))

    # Validate ray_activity_response field can be cast to RayActivityResponse object
    driver_ray_activity_response = RayActivityResponse(**data["driver"])
    print(driver_ray_activity_response)

    assert driver_ray_activity_response.is_active == "ACTIVE"
    # Drivers with namespace starting with "_ray_internal" are not
    # considered active drivers. Two active drivers are the second one
    # run with namespace "my_namespace" and the one started
    # from ray_start_with_dashboard
    assert driver_ray_activity_response.reason == "Number of active drivers: 2"

    # Get expected_last_activity at from snapshot endpoint which returns details
    # about all jobs
    jobs_snapshot_data = requests.get(f"{webui_url}/api/snapshot").json()["data"][
        "snapshot"
    ]["jobs"]

    print(jobs_snapshot_data)
    # Divide endTime by 1000 to convert from milliseconds to seconds
    expected_last_activity_at = max(
        [job.get("endTime", 0) / 1000 for (job_id, job) in jobs_snapshot_data.items()]
    )

    assert driver_ray_activity_response.last_activity_at == expected_last_activity_at


def test_snapshot(ray_start_with_dashboard):
    driver_template = """
import ray

ray.init(address="{address}", namespace="my_namespace")

@ray.remote
class Pinger:
    def ping(self):
        return "pong"

a = Pinger.options(lifetime={lifetime}, name={name}).remote()
ray.get(a.ping.remote())
    """
    address = ray_start_with_dashboard["address"]
    detached_driver = driver_template.format(
        address=address, lifetime="'detached'", name="'abc'"
    )
    named_driver = driver_template.format(
        address=address, lifetime="None", name="'xyz'"
    )
    unnamed_driver = driver_template.format(
        address=address, lifetime="None", name="None"
    )

    run_string_as_driver(detached_driver)
    run_string_as_driver(named_driver)
    run_string_as_driver(unnamed_driver)

    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    response = requests.get(f"{webui_url}/api/snapshot")
    response.raise_for_status()
    data = response.json()
    schema_path = os.path.join(
        os.path.dirname(dashboard.__file__), "modules/snapshot/snapshot_schema.json"
    )
    pprint.pprint(data)
    jsonschema.validate(instance=data, schema=json.load(open(schema_path)))

    assert len(data["data"]["snapshot"]["actors"]) == 3
    assert len(data["data"]["snapshot"]["jobs"]) == 4
    assert len(data["data"]["snapshot"]["deployments"]) == 0

    for actor_id, entry in data["data"]["snapshot"]["actors"].items():
        assert entry["jobId"] in data["data"]["snapshot"]["jobs"]
        assert entry["actorClass"] == "Pinger"
        assert entry["startTime"] >= 0
        if entry["isDetached"]:
            assert entry["endTime"] == 0, entry
        else:
            assert entry["endTime"] > 0, entry
        assert "runtimeEnv" in entry
    assert data["data"]["snapshot"]["rayCommit"] == ray.__commit__
    assert data["data"]["snapshot"]["rayVersion"] == ray.__version__

    # test actor limit
    response = requests.get(f"{webui_url}/api/snapshot?actor_limit=2")
    response.raise_for_status()
    data = response.json()
    pprint.pprint(data)
    assert len(data["data"]["snapshot"]["actors"]) == 2


def test_snapshot_timeout(monkeypatch, ray_start_cluster):
    """Verifies the timeout argument works for snapshot API."""
    with monkeypatch.context() as m:
        # defer for 5s for the second node.
        # This will help the API not return until the node is killed.
        m.setenv(
            "RAY_testing_asio_delay_us",
            "ActorInfoGcsService.grpc_server.GetAllActorInfo=2000000:2000000",
        )
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=2)
        address = ray.init(address=cluster.address)
        webui_url = address["webui_url"]

        # Verifies timeout will work.
        response = requests.get(f"http://{webui_url}/api/snapshot?timeout=1")
        assert response.json()["result"] is False
        assert "Deadline Exceeded" in response.json()["msg"]


@pytest.mark.parametrize("ray_start_with_dashboard", [{"num_cpus": 4}], indirect=True)
def test_serve_snapshot(ray_start_with_dashboard):
    """Test reconnecting to detached Serve application."""

    detached_serve_driver_script = f"""
import ray
from ray import serve

ray.init(
    address="{ray_start_with_dashboard['address']}",
    namespace="serve")

serve.start(detached=True)

@serve.deployment
def my_func(request):
  return "hello"

my_func.deploy()

@serve.deployment(version="v1")
def my_func_deleted(request):
  return "hello"

my_func_deleted.deploy()
my_func_deleted.delete()
    """

    run_string_as_driver(detached_serve_driver_script)
    assert requests.get("http://127.0.0.1:8000/my_func").text == "hello"

    # Connect to the running Serve application with detached=False.
    serve.start(detached=False)

    @serve.deployment(version="v1")
    def my_func_nondetached(request):
        return "hello"

    my_func_nondetached.deploy()

    assert requests.get("http://127.0.0.1:8000/my_func_nondetached").text == "hello"

    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    response = requests.get(f"{webui_url}/api/snapshot")
    response.raise_for_status()
    data = response.json()
    schema_path = os.path.join(
        os.path.dirname(dashboard.__file__), "modules/snapshot/snapshot_schema.json"
    )
    pprint.pprint(data)
    jsonschema.validate(instance=data, schema=json.load(open(schema_path)))

    assert len(data["data"]["snapshot"]["deployments"]) == 3

    entry = data["data"]["snapshot"]["deployments"][
        hashlib.sha1("my_func".encode()).hexdigest()
    ]
    assert entry["name"] == "my_func"
    assert entry["version"] is None
    assert entry["namespace"] == SERVE_NAMESPACE
    assert entry["httpRoute"] == "/my_func"
    assert entry["className"] == "my_func"
    assert entry["status"] == "RUNNING"
    assert entry["rayJobId"] is not None
    assert entry["startTime"] > 0
    assert entry["endTime"] == 0

    assert len(entry["actors"]) == 1
    actor_id = next(iter(entry["actors"]))
    metadata = data["data"]["snapshot"]["actors"][actor_id]["metadata"]["serve"]
    assert metadata["deploymentName"] == "my_func"
    assert metadata["version"] is None
    assert len(metadata["replicaTag"]) > 0

    entry_deleted = data["data"]["snapshot"]["deployments"][
        hashlib.sha1("my_func_deleted".encode()).hexdigest()
    ]
    assert entry_deleted["name"] == "my_func_deleted"
    assert entry_deleted["version"] == "v1"
    assert entry_deleted["namespace"] == SERVE_NAMESPACE
    assert entry_deleted["httpRoute"] is None
    assert entry_deleted["className"] == "my_func_deleted"
    assert entry_deleted["status"] == "DELETED"
    assert entry["rayJobId"] is not None
    assert entry_deleted["startTime"] > 0
    assert entry_deleted["endTime"] > entry_deleted["startTime"]

    entry_nondetached = data["data"]["snapshot"]["deployments"][
        hashlib.sha1("my_func_nondetached".encode()).hexdigest()
    ]
    assert entry_nondetached["name"] == "my_func_nondetached"
    assert entry_nondetached["version"] == "v1"
    assert entry_nondetached["namespace"] == SERVE_NAMESPACE
    assert entry_nondetached["httpRoute"] == "/my_func_nondetached"
    assert entry_nondetached["className"] == "my_func_nondetached"
    assert entry_nondetached["status"] == "RUNNING"
    assert entry_nondetached["rayJobId"] is not None
    assert entry_nondetached["startTime"] > 0
    assert entry_nondetached["endTime"] == 0

    assert len(entry_nondetached["actors"]) == 1
    actor_id = next(iter(entry_nondetached["actors"]))
    metadata = data["data"]["snapshot"]["actors"][actor_id]["metadata"]["serve"]
    assert metadata["deploymentName"] == "my_func_nondetached"
    assert metadata["version"] == "v1"
    assert len(metadata["replicaTag"]) > 0

    my_func_nondetached.delete()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
