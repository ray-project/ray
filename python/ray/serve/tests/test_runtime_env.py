import pytest
import sys

import ray
from ray import serve
from ray._private.test_utils import run_string_as_driver


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


@pytest.mark.parametrize("use_ray_client", [False, True])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_working_dir_basic(ray_start, tmp_dir, use_ray_client, ray_shutdown):
    with open("hello", "w") as f:
        f.write("world")
    print("Wrote file")
    if use_ray_client:
        ray.init(
            f"ray://{ray_start}", namespace="serve", runtime_env={"working_dir": "."}
        )
    else:
        ray.init(address="auto", namespace="serve", runtime_env={"working_dir": "."})
    print("Initialized Ray")
    serve.start()
    print("Started Serve")

    @serve.deployment
    class Test:
        def __call__(self, *args):
            return open("hello").read()

    Test.deploy()
    print("Deployed")
    handle = Test.get_handle()
    print("Got handle")
    assert ray.get(handle.remote()) == "world"


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
