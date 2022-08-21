import pytest
import sys

from ray._private.test_utils import run_string_as_driver


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_working_dir_deploy_new_version(ray_start, tmp_dir):
    with open("hello", "w") as f:
        f.write("world")

    driver1 = """
import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={"working_dir": "."})
ray.init(address="auto", namespace="serve", job_config=job_config)


@serve.deployment(version="1")
class Test:
    def __call__(self, *args):
        return open("hello").read()

handle = serve.run(Test.bind())
assert ray.get(handle.remote()) == "world"
"""

    run_string_as_driver(driver1)

    with open("hello", "w") as f:
        f.write("world2")

    driver2 = """
import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={"working_dir": "."})
ray.init(address="auto", namespace="serve", job_config=job_config)


@serve.deployment(version="2")
class Test:
    def __call__(self, *args):
        return open("hello").read()

handle = serve.run(Test.bind())
assert ray.get(handle.remote()) == "world2"
Test.delete()
"""

    run_string_as_driver(driver2)


@pytest.mark.skipif(
    sys.platform == "win32", reason="Runtime env unsupported on Windows"
)
def test_pip_no_working_dir(ray_start):

    driver = """
import ray
from ray import serve
import requests

ray.init(address="auto")


@serve.deployment
def requests_version(request):
    return requests.__version__


serve.run(requests_version.options(
    ray_actor_options={
        "runtime_env": {
            "pip": ["requests==2.25.1"]
        }
    }).bind())

assert requests.get("http://127.0.0.1:8000/requests_version").text == "2.25.1"
"""

    output = run_string_as_driver(driver)
    print(output)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
