import json
import subprocess
import sys

import pytest
import requests

from ray import serve


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


@serve.deployment
class A:
    def __init__(self, value, increment=1):
        self.value = value
        self.increment = increment
        self.decrement = 0

    def reconfigure(self, config):
        self.decrement = config["decrement"]

    def __call__(self, inp):
        return self.value + self.increment - self.decrement


def test_deploy(ray_start_stop):
    subprocess.check_output(["serve", "start"])
    subprocess.check_output([
        "serve", "deploy", "ray.serve.tests.test_cli.A", "--options-json",
        json.dumps({
            "name": "B",
            "init_args": [42],
            "init_kwargs": {
                "increment": 10
            },
            "num_replicas": 2,
            "user_config": {
                "decrement": 5
            },
        })
    ])
    resp = requests.get("http://127.0.0.1:8000/B")
    resp.raise_for_status()
    assert resp.text == "47", resp.text


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
