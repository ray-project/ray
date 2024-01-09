import sys

import pytest

import ray
from ray import serve
from ray._private.test_utils import run_string_as_driver


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_failure_condition(ray_start, tmp_dir):
    # Verify that the test conditions fail without passing the working dir.
    with open("hello", "w") as f:
        f.write("world")

    driver = """
import ray
from ray import serve

ray.init(address="auto")


@serve.deployment
class Test:
    def __call__(self, *args):
        return open("hello").read()

handle = serve.run(Test.bind())
try:
    handle.remote().result()
    assert False, "Should not get here"
except FileNotFoundError:
    pass
"""

    run_string_as_driver(driver)


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_working_dir_basic(ray_start, tmp_dir, ray_shutdown):
    with open("hello", "w") as f:
        f.write("world")
    print("Wrote file")

    ray.init(address="auto", namespace="serve", runtime_env={"working_dir": "."})
    print("Initialized Ray")

    @serve.deployment
    class Test:
        def __call__(self, *args):
            return open("hello").read()

    handle = serve.run(Test.bind())
    print("Deployed")

    assert handle.remote().result() == "world"


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_working_dir_connect_from_new_driver(ray_start, tmp_dir):
    with open("hello", "w") as f:
        f.write("world")

    driver1 = """
import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={"working_dir": "."})
ray.init(address="auto", namespace="serve", job_config=job_config)


@serve.deployment
class Test:
    def __call__(self, *args):
        return open("hello").read()

handle = serve.run(Test.bind(), name="app")
assert handle.remote().result() == "world"
"""

    run_string_as_driver(driver1)

    driver2 = driver1 + "\nserve.delete('app')"
    run_string_as_driver(driver2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
