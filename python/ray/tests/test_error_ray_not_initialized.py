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
        ray.get_dashboard_url,
        ray.jobs,
        lambda: ray.kill(None),  # Not valid API usage.
        ray.nodes,
        ray.objects,
        lambda: ray.put(1),
        lambda: ray.wait([])
    ]

    def test_exceptions_raised():
        for api_method in api_methods:
            print(api_method)
            with pytest.raises(
                    ray.exceptions.RaySystemError,
                    match="Ray has not been started yet."):
                api_method()

    test_exceptions_raised()

    # Make sure that the exceptions are still raised after Ray has been
    # started and shutdown.
    ray.init(num_cpus=0)
    ray.shutdown()

    test_exceptions_raised()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
