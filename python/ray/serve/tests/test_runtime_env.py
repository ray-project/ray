import os
import pytest
import sys
import random
import tempfile
import subprocess

import ray
from ray._private.test_utils import run_string_as_driver

# https://tools.ietf.org/html/rfc6335#section-6
MIN_DYNAMIC_PORT = 49152
MAX_DYNAMIC_PORT = 65535

if not os.environ.get("CI"):
    # This flag allows for local testing of runtime env conda/pip functionality
    # without needing a built Ray wheel.  It links to the current Python site.
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"


@pytest.fixture
def ray_start(scope="module"):
    port = random.randint(MIN_DYNAMIC_PORT, MAX_DYNAMIC_PORT)
    subprocess.check_output(
        [
            "ray",
            "start",
            "--head",
            "--num-cpus",
            "16",
            "--ray-client-server-port",
            f"{port}",
        ]
    )
    try:
        yield f"localhost:{port}"
    finally:
        subprocess.check_output(["ray", "stop", "--force"])


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        old_dir = os.getcwd()
        os.chdir(tmp_dir)
        yield tmp_dir
        os.chdir(old_dir)


@pytest.mark.parametrize("use_ray_client", [False, True])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_failure_condition(ray_start, tmp_dir, use_ray_client):
    # Verify that the test conditions fail without passing the working dir.
    with open("hello", "w") as f:
        f.write("world")

    driver = """
import ray
from ray import serve

if {use_ray_client}:
    ray.util.connect("{client_addr}")
else:
    ray.init(address="auto")

serve.start()

@serve.deployment
class Test:
    def __call__(self, *args):
        return open("hello").read()

Test.deploy()
handle = Test.get_handle()
try:
    ray.get(handle.remote())
    assert False, "Should not get here"
except FileNotFoundError:
    pass
""".format(
        use_ray_client=use_ray_client, client_addr=ray_start
    )

    run_string_as_driver(driver)


def connect_with_working_dir(use_ray_client: bool, ray_client_addr: str):
    job_config = ray.job_config.JobConfig(runtime_env={"working_dir": "."})
    if use_ray_client:
        ray.util.connect(ray_client_addr, namespace="serve", job_config=job_config)
    else:
        ray.init(address="auto", namespace="serve", job_config=job_config)


@pytest.mark.parametrize("use_ray_client", [False, True])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_working_dir_basic(ray_start, tmp_dir, use_ray_client):
    with open("hello", "w") as f:
        f.write("world")

    driver = """
import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={{"working_dir": "."}})
if {use_ray_client}:
    ray.util.connect("{client_addr}", job_config=job_config)
else:
    ray.init(address="auto", job_config=job_config)

serve.start()

@serve.deployment
class Test:
    def __call__(self, *args):
        return open("hello").read()

Test.deploy()
handle = Test.get_handle()
assert ray.get(handle.remote()) == "world"
""".format(
        use_ray_client=use_ray_client, client_addr=ray_start
    )

    run_string_as_driver(driver)


@pytest.mark.parametrize("use_ray_client", [False, True])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_working_dir_connect_from_new_driver(ray_start, tmp_dir, use_ray_client):
    with open("hello", "w") as f:
        f.write("world")

    driver1 = """
import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={{"working_dir": "."}})
if {use_ray_client}:
    ray.util.connect("{client_addr}", namespace="serve", job_config=job_config)
else:
    ray.init(address="auto", namespace="serve", job_config=job_config)

serve.start(detached=True)

@serve.deployment
class Test:
    def __call__(self, *args):
        return open("hello").read()

Test.deploy()
handle = Test.get_handle()
assert ray.get(handle.remote()) == "world"
""".format(
        use_ray_client=use_ray_client, client_addr=ray_start
    )

    run_string_as_driver(driver1)

    driver2 = """
import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={{"working_dir": "."}})
if {use_ray_client}:
    ray.util.connect("{client_addr}", namespace="serve", job_config=job_config)
else:
    ray.init(address="auto", namespace="serve", job_config=job_config)

serve.start(detached=True)

Test = serve.get_deployment("Test")
handle = Test.get_handle()
assert ray.get(handle.remote()) == "world"
Test.delete()
""".format(
        use_ray_client=use_ray_client, client_addr=ray_start
    )

    run_string_as_driver(driver2)


@pytest.mark.parametrize("use_ray_client", [False, True])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_working_dir_scale_up_in_new_driver(ray_start, tmp_dir, use_ray_client):
    with open("hello", "w") as f:
        f.write("world")

    driver1 = """
import os

import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={{"working_dir": "."}})
if {use_ray_client}:
    ray.util.connect("{client_addr}", namespace="serve", job_config=job_config)
else:
    ray.init(address="auto", namespace="serve", job_config=job_config)

serve.start(detached=True)

@serve.deployment(version="1")
class Test:
    def __call__(self, *args):
        return os.getpid(), open("hello").read()

Test.deploy()
handle = Test.get_handle()
assert ray.get(handle.remote())[1] == "world"
""".format(
        use_ray_client=use_ray_client, client_addr=ray_start
    )

    run_string_as_driver(driver1)

    with open("hello", "w") as f:
        f.write("no longer world")

    driver2 = """
import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={{"working_dir": "."}})
if {use_ray_client}:
    ray.util.connect("{client_addr}", namespace="serve", job_config=job_config)
else:
    ray.init(address="auto", namespace="serve", job_config=job_config)

serve.start(detached=True)

Test = serve.get_deployment("Test")
Test.options(num_replicas=2).deploy()
handle = Test.get_handle()
results = ray.get([handle.remote() for _ in range(1000)])
print(set(results))
assert all(r[1] == "world" for r in results), (
    "results should still come from the first env")
assert len(set(r[0] for r in results)) == 2, (
    "make sure there are two replicas")
Test.delete()
""".format(
        use_ray_client=use_ray_client, client_addr=ray_start
    )

    run_string_as_driver(driver2)


@pytest.mark.parametrize("use_ray_client", [False, True])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_working_dir_deploy_new_version(ray_start, tmp_dir, use_ray_client):
    with open("hello", "w") as f:
        f.write("world")

    driver1 = """
import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={{"working_dir": "."}})
if {use_ray_client}:
    ray.util.connect("{client_addr}", namespace="serve", job_config=job_config)
else:
    ray.init(address="auto", namespace="serve", job_config=job_config)

serve.start(detached=True)

@serve.deployment(version="1")
class Test:
    def __call__(self, *args):
        return open("hello").read()

Test.deploy()
handle = Test.get_handle()
assert ray.get(handle.remote()) == "world"
""".format(
        use_ray_client=use_ray_client, client_addr=ray_start
    )

    run_string_as_driver(driver1)

    with open("hello", "w") as f:
        f.write("world2")

    driver2 = """
import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={{"working_dir": "."}})
if {use_ray_client}:
    ray.util.connect("{client_addr}", namespace="serve", job_config=job_config)
else:
    ray.init(address="auto", namespace="serve", job_config=job_config)

serve.start(detached=True)

@serve.deployment(version="2")
class Test:
    def __call__(self, *args):
        return open("hello").read()

Test.deploy()
handle = Test.get_handle()
assert ray.get(handle.remote()) == "world2"
Test.delete()
""".format(
        use_ray_client=use_ray_client, client_addr=ray_start
    )

    run_string_as_driver(driver2)


@pytest.mark.parametrize("use_ray_client", [False, True])
@pytest.mark.skipif(
    sys.platform == "win32", reason="Runtime env unsupported on Windows"
)
@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Post-wheel-build test is only run on linux CI machines.",
)
def test_pip_no_working_dir(ray_start, use_ray_client):

    driver = """
import ray
from ray import serve
import requests

if {use_ray_client}:
    ray.util.connect("{client_addr}")
else:
    ray.init(address="auto")

serve.start()


@serve.deployment
def requests_version(request):
    return requests.__version__


requests_version.options(
    ray_actor_options={{
        "runtime_env": {{
            "pip": ["ray[serve]", "requests==2.25.1"]
        }}
    }}).deploy()

assert requests.get("http://127.0.0.1:8000/requests_version").text == "2.25.1"
""".format(
        use_ray_client=use_ray_client, client_addr=ray_start
    )

    run_string_as_driver(driver)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
