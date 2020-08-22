import sys

import pytest

import ray


def test_errors_before_initializing_ray():
    @ray.remote
    def f():
        pass

    @ray.remote
    class Foo:
        pass

    api_methods = [
        f.remote,
        Foo.remote,
        ray.actors,
        lambda: ray.cancel(None),  # Not valid API usage.
        lambda: ray.get([]),
        lambda: ray.get_actor("name"),
        ray.get_gpu_ids,
        ray.get_resource_ids,
        ray.get_webui_url,
        ray.jobs,
        lambda: ray.kill(None),  # Not valid API usage.
        ray.nodes,
        ray.objects,
        lambda: ray.put(1),
        lambda: ray.wait([])
    ]

    for api_method in api_methods:
        with pytest.raises(
                ray.exceptions.RayConnectionError,
                match="Ray has not been started yet."):
            api_method()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
