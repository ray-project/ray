import pytest
from contextlib import contextmanager

import ray.experimental.client.server.server as ray_client_server
from ray.experimental.client import ray
from ray.experimental.client.common import ClientObjectRef


@contextmanager
def ray_start_client_server():
    server = ray_client_server.serve("localhost:50051", test_mode=True)
    ray.connect("localhost:50051")
    yield ray
    ray.disconnect()
    server.stop(0)


def test_real_ray_fallback(ray_start_regular_shared):
    with ray_start_client_server() as ray:

        @ray.remote
        def get_nodes_real():
            import ray as real_ray
            return real_ray.nodes()

        nodes = ray.get(get_nodes_real.remote())
        assert len(nodes) == 1, nodes

        @ray.remote
        def get_nodes():
            # Can access the full Ray API in remote methods.
            return ray.nodes()

        nodes = ray.get(get_nodes.remote())
        assert len(nodes) == 1, nodes

        with pytest.raises(NotImplementedError):
            print(ray.nodes())


def test_nested_function(ray_start_regular_shared):
    with ray_start_client_server() as ray:

        @ray.remote
        def g():
            @ray.remote
            def f():
                return "OK"

            return ray.get(f.remote())

        assert ray.get(g.remote()) == "OK"


def test_put_get(ray_start_regular_shared):
    with ray_start_client_server() as ray:
        objectref = ray.put("hello world")
        print(objectref)

        retval = ray.get(objectref)
        assert retval == "hello world"


def test_wait(ray_start_regular_shared):
    with ray_start_client_server() as ray:
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


def test_remote_functions(ray_start_regular_shared):
    with ray_start_client_server() as ray:

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
        all_vals = ray.get(res[0])
        assert all_vals == [236, 2_432_902_008_176_640_000, 120, 3628800]


def test_function_calling_function(ray_start_regular_shared):
    with ray_start_client_server() as ray:

        @ray.remote
        def g():
            return "OK"

        @ray.remote
        def f():
            print(f, f._name, g._name, g)
            return ray.get(g.remote())

        print(f, type(f))
        assert ray.get(f.remote()) == "OK"


def test_basic_actor(ray_start_regular_shared):
    with ray_start_client_server() as ray:

        @ray.remote
        class HelloActor:
            def __init__(self):
                self.count = 0

            def say_hello(self, whom):
                self.count += 1
                return ("Hello " + whom, self.count)

        actor = HelloActor.remote()
        s, count = ray.get(actor.say_hello.remote("you"))
        assert s == "Hello you"
        assert count == 1
        s, count = ray.get(actor.say_hello.remote("world"))
        assert s == "Hello world"
        assert count == 2


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
