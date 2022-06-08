import os
import sys
import json
import time
import pytest
import requests
import subprocess
from typing import List, Dict, Set

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition


GET_OR_PUT_URL = "http://localhost:8265/api/serve/deployments/"
STATUS_URL = "http://localhost:8265/api/serve/deployments/status"
test_env_uri = "https://github.com/shrekris-anyscale/test_deploy_group/archive/HEAD.zip"
test_module_uri = "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"


@pytest.fixture
def ray_start_stop():
    subprocess.check_output(["ray", "start", "--head"])
    yield
    subprocess.check_output(["ray", "stop", "--force"])


def deployments_match(list1: List[Dict], list2: List[Dict], properties: Set) -> bool:
    """
    Helper that takes in 2 lists of deployment dictionaries and compares their
    properties and ray_actor_options.
    """

    if len(list1) != len(list2):
        return False

    for deployment1 in list1:
        matching_deployment = None
        for i in range(len(list2)):
            deployment2 = list2[i]
            for property in properties:
                if deployment1[property] != deployment2[property]:
                    break
            else:
                matching_deployment = i
        if matching_deployment is None:
            return False
        list2.pop(matching_deployment)

    return len(list2) == 0


@pytest.mark.skipif(
    sys.platform == "win32", reason="File paths incompatible with Windows."
)
def test_put_get_success(ray_start_stop):
    ray_actor_options = {
        "runtime_env": {"py_modules": [test_env_uri, test_module_uri]},
        "num_cpus": 0.1,
    }

    shallow = dict(
        name="shallow",
        num_replicas=3,
        route_prefix="/shallow",
        ray_actor_options=ray_actor_options,
        import_path="test_env.shallow_import.ShallowClass",
    )

    deep = dict(
        name="deep",
        route_prefix="/deep",
        ray_actor_options=ray_actor_options,
        import_path="test_env.subdir1.subdir2.deep_import.DeepClass",
    )

    one = dict(
        name="one",
        num_replicas=3,
        route_prefix="/one",
        ray_actor_options=ray_actor_options,
        import_path="test_module.test.one",
    )

    three_deployments = os.path.join(
        os.path.dirname(__file__), "three_deployments_response.json"
    )

    two_deployments = os.path.join(
        os.path.dirname(__file__), "two_deployments_response.json"
    )

    # Ensure the REST API is idempotent
    for _ in range(2):
        deployments = [shallow, deep, one]

        put_response = requests.put(
            GET_OR_PUT_URL, json={"deployments": deployments}, timeout=30
        )
        assert put_response.status_code == 200
        assert (
            requests.get("http://localhost:8000/shallow", timeout=30).text
            == "Hello shallow world!"
        )
        assert (
            requests.get("http://localhost:8000/deep", timeout=30).text
            == "Hello deep world!"
        )
        assert requests.get("http://localhost:8000/one", timeout=30).text == "2"

        get_response = requests.get(GET_OR_PUT_URL, timeout=30)
        assert get_response.status_code == 200

        with open(three_deployments, "r") as f:
            response_deployments = get_response.json()["deployments"]
            expected_deployments = json.load(f)["deployments"]
            assert deployments_match(
                response_deployments,
                expected_deployments,
                {"name", "import_path", "num_replicas", "route_prefix"},
            )

        deployments = [shallow, one]
        put_response = requests.put(
            GET_OR_PUT_URL, json={"deployments": deployments}, timeout=30
        )
        assert put_response.status_code == 200

        # Use wait_for_condition() to ensure "deep" deployment deleted
        wait_for_condition(
            lambda: len(requests.get(GET_OR_PUT_URL, timeout=3).json()["deployments"])
            == 2,
            timeout=10,
        )

        assert (
            requests.get("http://localhost:8000/shallow", timeout=30).text
            == "Hello shallow world!"
        )
        assert requests.get("http://localhost:8000/deep", timeout=30).status_code == 404
        assert requests.get("http://localhost:8000/one", timeout=30).text == "2"

        get_response = requests.get(GET_OR_PUT_URL, timeout=30)
        assert get_response.status_code == 200

        with open(two_deployments, "r") as f:
            response_deployments = get_response.json()["deployments"]
            expected_deployments = json.load(f)["deployments"]
            assert deployments_match(
                response_deployments,
                expected_deployments,
                {"name", "import_path", "num_replicas", "route_prefix"},
            )


def test_put_new_rest_api(ray_start_stop):
    config = {
        "import_path": "conditional_dag.serve_dag",
        "runtime_env": {
            "working_dir": (
                "https://github.com/ray-project/test_dag/archive/"
                "cc246509ba3c9371f8450f74fdc18018428630bd.zip"
            )
        },
        "deployments": [
            {
                "name": "Multiplier",
                "user_config": {"factor": 1},
            },
            {
                "name": "Adder",
                "ray_actor_options": {
                    "runtime_env": {"env_vars": {"override_increment": "1"}}
                },
            },
        ],
    }

    put_response = requests.put(GET_OR_PUT_URL, json=config, timeout=30)
    assert put_response.status_code == 200
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
        == "3 pizzas please!",
        timeout=15,
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["MUL", 2]).json()
        == "-4 pizzas please!",
        timeout=15,
    )

    # Make Adder's ray_actor_options an empty dictionary.
    config["deployments"][1]["ray_actor_options"] = {}

    # Check that Adder's empty config ray_actor_options override its code options
    put_response = requests.put(GET_OR_PUT_URL, json=config, timeout=30)
    assert put_response.status_code == 200
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
        == "4 pizzas please!",
        timeout=15,
    )


def test_delete_success(ray_start_stop):
    ray_actor_options = {
        "runtime_env": {
            "working_dir": (
                "https://github.com/shrekris-anyscale/"
                "test_deploy_group/archive/HEAD.zip"
            )
        }
    }

    shallow = dict(
        name="shallow",
        num_replicas=3,
        route_prefix="/shallow",
        ray_actor_options=ray_actor_options,
        import_path="test_env.shallow_import.ShallowClass",
    )

    # Ensure the REST API is idempotent
    for _ in range(2):
        put_response = requests.put(
            GET_OR_PUT_URL, json={"deployments": [shallow]}, timeout=30
        )
        assert put_response.status_code == 200
        assert (
            requests.get("http://localhost:8000/shallow", timeout=30).text
            == "Hello shallow world!"
        )

        delete_response = requests.delete(GET_OR_PUT_URL, timeout=30)
        assert delete_response.status_code == 200

        # Make sure all deployments are deleted
        wait_for_condition(
            lambda: len(requests.get(GET_OR_PUT_URL, timeout=3).json()["deployments"])
            == 0,
            timeout=10,
        )


def test_get_status_info(ray_start_stop):
    ray_actor_options = {"runtime_env": {"py_modules": [test_env_uri, test_module_uri]}}

    shallow = dict(
        name="shallow",
        num_replicas=3,
        route_prefix="/shallow",
        ray_actor_options=ray_actor_options,
        import_path="test_env.shallow_import.ShallowClass",
    )

    deep = dict(
        name="deep",
        route_prefix="/deep",
        ray_actor_options=ray_actor_options,
        import_path="test_env.subdir1.subdir2.deep_import.DeepClass",
    )

    one = dict(
        name="one",
        num_replicas=3,
        route_prefix="/one",
        ray_actor_options=ray_actor_options,
        import_path="test_module.test.one",
    )

    deployments = [shallow, deep, one]

    put_response = requests.put(
        GET_OR_PUT_URL, json={"deployments": deployments}, timeout=30
    )
    assert put_response.status_code == 200

    status_response = requests.get(STATUS_URL, timeout=30)
    assert status_response.status_code == 200
    serve_status = status_response.json()

    deployment_statuses = serve_status["deployment_statuses"]
    assert len(deployment_statuses) == len(deployments)
    expected_deployment_names = {deployment["name"] for deployment in deployments}
    for deployment_status in deployment_statuses:
        assert deployment_status["name"] in expected_deployment_names
        expected_deployment_names.remove(deployment_status["name"])
        assert deployment_status["status"] in {"UPDATING", "HEALTHY"}
        assert deployment_status["message"] == ""
    assert len(expected_deployment_names) == 0

    assert serve_status["app_status"]["status"] in {"DEPLOYING", "RUNNING"}
    wait_for_condition(
        lambda: time.time() > serve_status["app_status"]["deployment_timestamp"],
        timeout=2,
    )


def test_serve_namespace(ray_start_stop):
    """
    Check that the Dashboard's Serve can interact with the Python API
    when they both start in the "serve namespace"
    """

    one = dict(
        name="one",
        num_replicas=1,
        route_prefix="/one",
        ray_actor_options={"runtime_env": {"py_modules": [test_module_uri]}},
        import_path="test_module.test.one",
    )
    put_response = requests.put(GET_OR_PUT_URL, json={"deployments": [one]}, timeout=30)
    assert put_response.status_code == 200
    ray.init(address="auto", namespace="serve")
    serve.start()
    deployments = serve.list_deployments()
    assert len(deployments) == 1
    assert "one" in deployments
    serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
