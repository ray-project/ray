import pytest
import sys

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
    ray.get(handle.remote())
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

    assert ray.get(handle.remote()) == "world"


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

handle = serve.run(Test.bind())
assert ray.get(handle.remote()) == "world"
"""

    run_string_as_driver(driver1)

    driver2 = """
import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={"working_dir": "."})

ray.init(address="auto", namespace="serve", job_config=job_config)

Test = serve.get_deployment("Test")
handle = serve.run(Test.bind())
assert ray.get(handle.remote()) == "world"
Test.delete()
"""

    run_string_as_driver(driver2)


# NOTE: This test uses deployment.deploy() instead of serve.run() to preserve
# the cached runtime_env that's returned by serve.get_deployment().
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_working_dir_scale_up_in_new_driver(ray_start, tmp_dir):
    with open("hello", "w") as f:
        f.write("world")

    driver1 = """
import os

import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={"working_dir": "."})
ray.init(address="auto", namespace="serve", job_config=job_config)
serve.start(detached=True)

@serve.deployment(version="1")
class Test:
    def __call__(self, *args):
        return os.getpid(), open("hello").read()

Test.deploy()
handle = Test.get_handle()
assert ray.get(handle.remote())[1] == "world"
"""

    run_string_as_driver(driver1)

    with open("hello", "w") as f:
        f.write("no longer world")

    driver2 = """
import ray
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={"working_dir": "."})
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
"""

    run_string_as_driver(driver2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
