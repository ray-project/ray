import ray
import ray.test_utils
from ray import serve


def test_new_driver(serve_instance):
    script = """
import ray
ray.init(address="{}")

from ray import serve

def driver(starlette_request):
    return "OK!"

serve.create_backend("driver", driver)
serve.create_endpoint("driver", backend="driver", route="/driver")
""".format(ray.worker._global_node._redis_address)
    ray.test_utils.run_string_as_driver(script)

    handle = serve.get_handle("driver")
    assert ray.get(handle.remote()) == "OK!"


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
