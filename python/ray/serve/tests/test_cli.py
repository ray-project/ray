import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
import requests

import ray
from ray import serve
from ray.tests.conftest import tmp_working_dir  # noqa: F401, E501
from ray.dashboard.optional_utils import RAY_INTERNAL_DASHBOARD_NAMESPACE


@pytest.fixture
def ray_start_stop():
    subprocess.check_output(["ray", "start", "--head"])
    yield
    subprocess.check_output(["ray", "stop", "--force"])


def test_start_shutdown(ray_start_stop):
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_output(["serve", "shutdown"])

    subprocess.check_output(["serve", "start"])
    subprocess.check_output(["serve", "shutdown"])


def test_start_shutdown_in_namespace(ray_start_stop):
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_output(["serve", "-n", "test", "shutdown"])

    subprocess.check_output(["serve", "-n", "test", "start"])
    subprocess.check_output(["serve", "-n", "test", "shutdown"])


class A:
    def __init__(self, value, increment=1):
        self.value = value
        self.increment = increment
        self.decrement = 0
        self.multiplier = int(os.environ["SERVE_TEST_MULTIPLIER"])

        p = Path("hello")
        assert p.exists()
        with open(p) as f:
            assert f.read() == "world"

    def reconfigure(self, config):
        self.decrement = config["decrement"]

    def __call__(self, inp):
        return (self.value + self.increment - self.decrement) * self.multiplier


@serve.deployment
class DecoratedA(A):
    pass


@pytest.mark.parametrize("class_name", ["A", "DecoratedA"])
def test_create_deployment(ray_start_stop, tmp_working_dir, class_name):  # noqa: F811
    subprocess.check_output(["serve", "start"])
    subprocess.check_output(
        [
            "serve",
            "--runtime-env-json",
            json.dumps(
                {
                    "working_dir": tmp_working_dir,
                }
            ),
            "create-deployment",
            f"ray.serve.tests.test_cli.{class_name}",
            "--options-json",
            json.dumps(
                {
                    "name": "B",
                    "init_args": [42],
                    "init_kwargs": {"increment": 10},
                    "num_replicas": 2,
                    "user_config": {"decrement": 5},
                    "ray_actor_options": {
                        "runtime_env": {
                            "env_vars": {
                                "SERVE_TEST_MULTIPLIER": "2",
                            },
                        }
                    },
                }
            ),
        ]
    )
    resp = requests.get("http://127.0.0.1:8000/B")
    resp.raise_for_status()
    assert resp.text == "94", resp.text


def test_deploy(ray_start_stop):
    # Deploys two valid config files and checks that the deployments work
    subprocess.check_output(["serve", "start"])

    # Initialize serve in test to enable calling serve.list_deployments()
    ray.init(address="auto", namespace=RAY_INTERNAL_DASHBOARD_NAMESPACE)
    serve.start(detached=True)

    # Dictionary mapping test config file names to expected deployment names
    # and configurations. These should match the values specified in the YAML
    # files.
    configs = {
        "test_config_files/three_deployments.yaml": {
            "shallow": {
                "num_replicas": 1,
                "response": "Hello shallow world!",
            },
            "deep": {
                "num_replicas": 1,
                "response": "Hello deep world!",
            },
            "one": {
                "num_replicas": 2,
                "response": "2",
            },
        },
        "test_config_files/two_deployments.yaml": {
            "shallow": {
                "num_replicas": 3,
                "response": "Hello shallow world!",
            },
            "one": {
                "num_replicas": 2,
                "response": "2",
            },
        }
    }

    request_url = "http://localhost:8000/"
    success_message_fragment = b"Sent deployment request successfully!"
    for config_file_name, expected_deployments in configs.items():
        deploy_response = subprocess.check_output(["serve", "deploy", config_file_name])
        assert success_message_fragment in deploy_response

        running_deployments = serve.list_deployments()

        # Check that running deployment names match expected deployment names
        assert set(running_deployments.keys()) == expected_deployments.keys()

        for name, deployment in running_deployments.items():
            assert deployment.num_replicas == expected_deployments[name]["num_replicas"]
            
        for name, deployment_config in expected_deployments.items():
            assert requests.get(f"{request_url}{name}").text == deployment_config["response"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
