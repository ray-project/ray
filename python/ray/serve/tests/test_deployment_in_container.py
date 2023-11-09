import subprocess
import sys

import pytest
import requests

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_dummy():
    output = subprocess.check_output(["podman", "info"])
    assert False, output


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
# @pytest.mark.skip()
def test_actor_in_container():
    runtime_env = {
        "container": {
            # "image": "rayproject/ray:nightly-py39-cpu",
            "image": "zcin/runtime-env-prototype:latest",
            "driver": "podman",
            "worker_path": "/home/ray/anaconda3/lib/python3.9/site-packages/ray/_private/workers/default_worker.py",  # noqa
        }
    }

    @ray.remote(runtime_env=runtime_env)
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

        def get_counter(self):
            return self.value

    a = Counter.remote()
    a.increment.remote()
    assert ray.get(a.get_counter.remote()) == 1
    ray.shutdown()


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
# @pytest.mark.skip()
def test_deployment_in_container():
    runtime_env = {
        "container": {
            # "image": "rayproject/ray:nightly-py39-cpu",
            "image": "zcin/runtime-env-prototype:latest",
            "driver": "podman",
            "worker_path": "/home/ray/anaconda3/lib/python3.9/site-packages/ray/_private/workers/default_worker.py",  # noqa
        }
    }

    @serve.deployment(ray_actor_options={"runtime_env": runtime_env})
    class Model:
        def __init__(self, name):
            self.name = name

        def __call__(self):
            return f"Good morning {self.name}!"

    app = Model.bind("Bob")
    serve.run(app)

    def check_app_running():
        status = serve.status().applications["default"]
        assert status.status == "RUNNING"
        return True

    wait_for_condition(check_app_running)

    assert requests.post("http://localhost:8000/").text == "Good morning Bob!"


if __name__ == "__main__":
    import os

    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
