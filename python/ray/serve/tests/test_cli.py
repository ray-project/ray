import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
import requests

from ray import serve
from ray.tests.conftest import tmp_working_dir  # noqa: F401, E501


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
def test_deploy(ray_start_stop, tmp_working_dir, class_name):  # noqa: F811
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
            "deploy",
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
