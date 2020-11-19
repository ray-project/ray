import pytest
import ray.experimental.client.server as ray_client_server
import ray.experimental.client as ray
from ray.experimental.client.common import ClientObjectRef


def test_put_get(ray_start_regular_shared):
    server = ray_client_server.serve("localhost:50051")
    ray.connect("localhost:50051")

    objectref = ray.put("hello world")
    print(objectref)

    retval = ray.get(objectref)
    assert retval == "hello world"
    ray.disconnect()
    server.stop(0)


def test_wait(ray_start_regular_shared):
    server = ray_client_server.serve("localhost:50051")
    ray.connect("localhost:50051")

    objectref = ray.put("hello world")
    ready, remaining = ray.wait([objectref])
    assert remaining == []
    retval = ray.get(ready[0])
    assert retval == "hello world"

    objectref2 = ray.put(5)
    ready, remaining = ray.wait([objectref, objectref2])
    assert (ready, remaining) == ([objectref], [objectref2]) or \
        (ready, remaining) == ([objectref2], [objectref])
    ready_retval = ray.get(ready[0])
    remaining_retval = ray.get(remaining[0])
    assert (ready_retval, remaining_retval) == ("hello world", 5) \
        or (ready_retval, remaining_retval) == (5, "hello world")

    with pytest.raises(Exception):
        # Reference not in the object store.
        ray.wait([ClientObjectRef("blabla")])
    with pytest.raises(AssertionError):
        ray.wait("blabla")
    with pytest.raises(AssertionError):
        ray.wait(ClientObjectRef("blabla"))
    with pytest.raises(AssertionError):
        ray.wait(["blabla"])

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

    # Test ray.wait()
    ref5 = fact.remote(10)
    # should return ref2, ref3, ref4
    res = ray.wait([ref5, ref2, ref3, ref4], num_returns=3)
    assert [ref2, ref3, ref4] == res[0]
    assert [ref5] == res[1]
    assert ray.get(res[0]) == [236, 2_432_902_008_176_640_000, 120]
    # should return ref2, ref3, ref4, ref5
    res = ray.wait([ref2, ref3, ref4, ref5], num_returns=4)
    assert [ref2, ref3, ref4, ref5] == res[0]
    assert [] == res[1]
    assert ray.get(res[0]) == [236, 2_432_902_008_176_640_000, 120, 3628800]

    ray.disconnect()
    server.stop(0)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
