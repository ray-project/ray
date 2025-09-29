import ray
from ray import serve
from ray._private.test_utils import run_string_as_driver


def test_new_driver(serve_instance):
    run_string_as_driver(
        """
import ray
ray.init(address="{}", namespace="default_test_namespace")

from ray import serve

@serve.deployment
def driver():
    return "OK!"

serve.run(driver.bind(), name="app")
""".format(
            ray.get_runtime_context().gcs_address,
        )
    )

    handle = serve.get_app_handle("app")
    assert handle.remote().result() == "OK!"


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
