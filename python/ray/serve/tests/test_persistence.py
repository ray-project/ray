import ray
from ray._private.test_utils import run_string_as_driver
from ray import serve


def test_new_driver(serve_instance):
    script = """
import ray
ray.init(address="{}", namespace="default_test_namespace")

from ray import serve

@serve.deployment
def driver():
    return "OK!"

driver.deploy()
""".format(
        ray.worker._global_node.address
    )
    run_string_as_driver(script)

    handle = serve.get_deployment("driver").get_handle()
    assert ray.get(handle.remote()) == "OK!"


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
