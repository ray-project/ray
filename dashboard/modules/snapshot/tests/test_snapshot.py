import os
import json
import jsonschema

import pprint
import requests

from ray.test_utils import (
    format_web_url,
    run_string_as_driver,
)
from ray.new_dashboard import dashboard
from ray.new_dashboard.tests.conftest import *  # noqa


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

    detached_driver = driver_template.format(
        address=ray_start_with_dashboard["redis_address"],
        lifetime="'detached'",
        name="'abc'")
    named_driver = driver_template.format(
        address=ray_start_with_dashboard["redis_address"],
        lifetime="None",
        name="'xyz'")
    unnamed_driver = driver_template.format(
        address=ray_start_with_dashboard["redis_address"],
        lifetime="None",
        name="None")

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

    for actor_id, entry in data["data"]["snapshot"]["actors"].items():
        assert entry["jobId"] in data["data"]["snapshot"]["jobs"]
        assert entry["actorClass"] == "Pinger"
        assert entry["startTime"] >= 0
        if entry["isDetached"]:
            assert entry["endTime"] == 0, entry
        else:
            assert entry["endTime"] > 0, entry
        assert "runtimeEnv" in entry


def test_serve_snapshot(ray_start_with_dashboard):
    driver_script = f"""
import ray
from ray import serve

ray.init(
    address="{ray_start_with_dashboard['redis_address']}",
    namespace="serve")

serve.start(detached=True)

@serve.deployment(version="v1")
def my_func(request):
  return "hello"

my_func.deploy()
    """
    run_string_as_driver(driver_script)

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

    assert len(data["data"]["snapshot"]["deployments"]) == 1
    for deployment_name, entry in data["data"]["snapshot"][
            "deployments"].items():
        assert entry["name"] == "my_func"
        assert entry["version"] == "v1"
        assert entry["httpRoute"] == "/my_func"
        assert entry["className"] == "my_func"
        assert entry["status"] == "RUNNING"
        assert entry["rayJobId"] is not None
        assert entry["startTime"] == 0
        assert entry["endTime"] == 0
