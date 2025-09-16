import json
import os
import pprint
import sys

import jsonschema
import pytest
import requests

from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import (
    format_web_url,
    run_string_as_driver,
    run_string_as_driver_nonblocking,
)
from ray.dashboard import dashboard
from ray.dashboard.consts import RAY_CLUSTER_ACTIVITY_HOOK
from ray.dashboard.modules.job.job_head import RayActivityResponse
from ray.dashboard.tests.conftest import *  # noqa


@pytest.fixture
def set_ray_cluster_activity_hook(request):
    """
    Fixture that sets RAY_CLUSTER_ACTIVITY_HOOK environment variable
    for test_e2e_component_activities_hook.
    """
    external_hook = request.param
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
        "modules/job/component_activities_schema.json",
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
    run_string_as_driver(
        driver_template.format(namespace="my_namespace", sleep_time_s=0)
    )

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

    def verify_driver_response():
        # Verify drivers are considered active after running script
        response = requests.get(f"{webui_url}/api/component_activities")
        response.raise_for_status()

        # Validate schema of response
        data = response.json()
        schema_path = os.path.join(
            os.path.dirname(dashboard.__file__),
            "modules/job/component_activities_schema.json",
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

        return True

    wait_for_condition(verify_driver_response)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
