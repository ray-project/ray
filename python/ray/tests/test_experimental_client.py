import pytest
import ray.experimental.client.server as ray_client_server
import ray.experimental.client as ray


def test_put_get(ray_start_regular_shared):
    server = ray_client_server.serve("localhost:50051")
    ray.connect("localhost:50051")

    objectref = ray.put("hello world")
    print(objectref)

    retval = ray.get(objectref)
    assert retval == "hello world"
    ray.disconnect()
    server.stop(0)


def test_remote_functions(ray_start_regular_shared):
    server = ray_client_server.serve("localhost:50051")

    @ray.remote
    def plus2(x):
        return x + 2

    @ray.remote
    def fact(x):
        print(x, type(fact))
        if x <= 0:
            return 1
        # This hits the "nested tasks" issue
        # https://github.com/ray-project/ray/issues/3644
        # So we're on the right track!
        return ray.get(fact.remote(x - 1)) * x

    ref2 = plus2.remote(234)
    # `236`
    assert ray.get(ref2) == 236

    ref3 = fact.remote(20)
    # `2432902008176640000`
    assert ray.get(ref3) == 2_432_902_008_176_640_000

    # Reuse the cached ClientRemoteFunc object
    ref4 = fact.remote(5)
    assert ray.get(ref4) == 120

    ray.disconnect()
    server.stop(0)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
