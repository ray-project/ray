import pytest
import sys

from ray._private.test_utils import run_string_as_driver


@pytest.mark.parametrize("use_ray_client", [False, True])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_working_dir_deploy_new_version(ray_start, tmp_dir, use_ray_client):
    with open("hello", "w") as f:
        f.write("world")

    driver1 = """
import ray
print("driver1:1")
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={{"working_dir": "."}})
if {use_ray_client}:
    ray.util.connect("{client_addr}", namespace="serve", job_config=job_config)
else:
    ray.init(address="auto", namespace="serve", job_config=job_config)
print("driver1:2")
serve.start(detached=True)
print("driver1:3")

@serve.deployment(version="1")
class Test:
    def __call__(self, *args):
        return open("hello").read()
print("driver1:4")
Test.deploy()
print("driver1:5")
handle = Test.get_handle()
print("driver1:6")
assert ray.get(handle.remote()) == "world"
""".format(
        use_ray_client=use_ray_client, client_addr=ray_start
    )

    print("test:1")
    run_string_as_driver(driver1)
    print("test:2")

    with open("hello", "w") as f:
        f.write("world2")

    driver2 = """
import ray
print("driver2:1")
from ray import serve

job_config = ray.job_config.JobConfig(runtime_env={{"working_dir": "."}})
print("driver2:2")
if {use_ray_client}:
    ray.util.connect("{client_addr}", namespace="serve", job_config=job_config)
else:
    ray.init(address="auto", namespace="serve", job_config=job_config)
print("driver2:3")
serve.start(detached=True)
print("driver2:4")
@serve.deployment(version="2")
class Test:
    def __call__(self, *args):
        return open("hello").read()
print("driver2:5")
Test.deploy()
print("driver2:6")
handle = Test.get_handle()
print("driver2:7")
assert ray.get(handle.remote()) == "world2"
print("driver2:8")
Test.delete()
""".format(
        use_ray_client=use_ray_client, client_addr=ray_start
    )

    print("test:3")
    run_string_as_driver(driver2)
    print("test:4")


@pytest.mark.parametrize("use_ray_client", [False, True])
@pytest.mark.skipif(
    sys.platform == "win32", reason="Runtime env unsupported on Windows"
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
