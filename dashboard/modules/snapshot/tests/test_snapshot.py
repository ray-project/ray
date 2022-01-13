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
from ray._private.test_utils import (
    format_web_url,
    run_string_as_driver,
)
from ray.dashboard import dashboard
from ray.dashboard.tests.conftest import *  # noqa


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
        address=address, lifetime="'detached'", name="'abc'")
    named_driver = driver_template.format(
        address=address, lifetime="None", name="'xyz'")
    unnamed_driver = driver_template.format(
        address=address, lifetime="None", name="None")

    run_string_as_driver(detached_driver)
    run_string_as_driver(named_driver)
    run_string_as_driver(unnamed_driver)

    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    response = requests.get(f"{webui_url}/api/snapshot")
    response.raise_for_status()
    data = response.json()
    schema_path = os.path.join(
        os.path.dirname(dashboard.__file__),
        "modules/snapshot/snapshot_schema.json")
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


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "num_cpus": 4
    }], indirect=True)
def test_serve_snapshot(ray_start_with_dashboard):
    """Test detached and nondetached Serve instances running concurrently."""

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

    # Use a new port to avoid clobbering the first Serve instance.
    serve.start(http_options={"port": 8123})

    @serve.deployment(version="v1")
    def my_func_nondetached(request):
        return "hello"

    my_func_nondetached.deploy()

    assert requests.get(
        "http://127.0.0.1:8123/my_func_nondetached").text == "hello"

    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    response = requests.get(f"{webui_url}/api/snapshot")
    response.raise_for_status()
    data = response.json()
    schema_path = os.path.join(
        os.path.dirname(dashboard.__file__),
        "modules/snapshot/snapshot_schema.json")
    pprint.pprint(data)
    jsonschema.validate(instance=data, schema=json.load(open(schema_path)))

    assert len(data["data"]["snapshot"]["deployments"]) == 3

    entry = data["data"]["snapshot"]["deployments"][hashlib.sha1(
        "my_func".encode()).hexdigest()]
    assert entry["name"] == "my_func"
    assert entry["version"] is None
    assert entry["namespace"] == "serve"
    assert entry["httpRoute"] == "/my_func"
    assert entry["className"] == "my_func"
    assert entry["status"] == "RUNNING"
    assert entry["rayJobId"] is not None
    assert entry["startTime"] > 0
    assert entry["endTime"] == 0

    assert len(entry["actors"]) == 1
    actor_id = next(iter(entry["actors"]))
    metadata = data["data"]["snapshot"]["actors"][actor_id]["metadata"][
        "serve"]
    assert metadata["deploymentName"] == "my_func"
    assert metadata["version"] is None
    assert len(metadata["replicaTag"]) > 0

    entry_deleted = data["data"]["snapshot"]["deployments"][hashlib.sha1(
        "my_func_deleted".encode()).hexdigest()]
    assert entry_deleted["name"] == "my_func_deleted"
    assert entry_deleted["version"] == "v1"
    assert entry_deleted["namespace"] == "serve"
    assert entry_deleted["httpRoute"] is None
    assert entry_deleted["className"] == "my_func_deleted"
    assert entry_deleted["status"] == "DELETED"
    assert entry["rayJobId"] is not None
    assert entry_deleted["startTime"] > 0
    assert entry_deleted["endTime"] > entry_deleted["startTime"]

    entry_nondetached = data["data"]["snapshot"]["deployments"][hashlib.sha1(
        "my_func_nondetached".encode()).hexdigest()]
    assert entry_nondetached["name"] == "my_func_nondetached"
    assert entry_nondetached["version"] == "v1"
    assert entry_nondetached["namespace"] == "default_test_namespace"
    assert entry_nondetached["httpRoute"] == "/my_func_nondetached"
    assert entry_nondetached["className"] == "my_func_nondetached"
    assert entry_nondetached["status"] == "RUNNING"
    assert entry_nondetached["rayJobId"] is not None
    assert entry_nondetached["startTime"] > 0
    assert entry_nondetached["endTime"] == 0

    assert len(entry_nondetached["actors"]) == 1
    actor_id = next(iter(entry_nondetached["actors"]))
    metadata = data["data"]["snapshot"]["actors"][actor_id]["metadata"][
        "serve"]
    assert metadata["deploymentName"] == "my_func_nondetached"
    assert metadata["version"] == "v1"
    assert len(metadata["replicaTag"]) > 0

    my_func_nondetached.delete()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
