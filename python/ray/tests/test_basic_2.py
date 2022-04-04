# coding: utf-8
import logging
import sys
import time

import numpy as np
import pytest

from ray._private.test_utils import client_test_enabled
from ray.exceptions import RayTaskError

if client_test_enabled():
    from ray.util.client import ray
else:
    import ray

logger = logging.getLogger(__name__)


def test_variable_number_of_args(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def varargs_fct1(*a):
        return " ".join(map(str, a))

    @ray.remote
    def varargs_fct2(a, *b):
        return " ".join(map(str, b))

    x = varargs_fct1.remote(0, 1, 2)
    assert ray.get(x) == "0 1 2"
    x = varargs_fct2.remote(0, 1, 2)
    assert ray.get(x) == "1 2"

    @ray.remote
    def f1(*args):
        return args

    @ray.remote
    def f2(x, y, *args):
        return x, y, args

    assert ray.get(f1.remote()) == ()
    assert ray.get(f1.remote(1)) == (1,)
    assert ray.get(f1.remote(1, 2, 3)) == (1, 2, 3)
    with pytest.raises(TypeError):
        f2.remote()
    with pytest.raises(TypeError):
        f2.remote(1)
    assert ray.get(f2.remote(1, 2)) == (1, 2, ())
    assert ray.get(f2.remote(1, 2, 3)) == (1, 2, (3,))
    assert ray.get(f2.remote(1, 2, 3, 4)) == (1, 2, (3, 4))

    def testNoArgs(self):
        @ray.remote
        def no_op():
            pass

        self.ray_start()

        ray.get(no_op.remote())


def test_defining_remote_functions(shutdown_only):
    ray.init(num_cpus=3)

    # Test that we can close over plain old data.
    data = [
        np.zeros([3, 5]),
        (1, 2, "a"),
        [0.0, 1.0, 1 << 62],
        1 << 60,
        {"a": np.zeros(3)},
    ]

    @ray.remote
    def g():
        return data

    ray.get(g.remote())

    # Test that we can close over modules.
    @ray.remote
    def h():
        return np.zeros([3, 5])

    assert np.alltrue(ray.get(h.remote()) == np.zeros([3, 5]))

    @ray.remote
    def j():
        return time.time()

    ray.get(j.remote())

    # Test that we can define remote functions that call other remote
    # functions.
    @ray.remote
    def k(x):
        return x + 1

    @ray.remote
    def k2(x):
        return ray.get(k.remote(x))

    @ray.remote
    def m(x):
        return ray.get(k2.remote(x))

    assert ray.get(k.remote(1)) == 2
    assert ray.get(k2.remote(1)) == 2
    assert ray.get(m.remote(1)) == 2


def test_redefining_remote_functions(shutdown_only):
    ray.init(num_cpus=1)

    # Test that we can define a remote function in the shell.
    @ray.remote
    def f(x):
        return x + 1

    assert ray.get(f.remote(0)) == 1

    # Test that we can redefine the remote function.
    @ray.remote
    def f(x):
        return x + 10

    while True:
        val = ray.get(f.remote(0))
        assert val in [1, 10]
        if val == 10:
            break
        else:
            logger.info("Still using old definition of f, trying again.")

    # Check that we can redefine functions even when the remote function source
    # doesn't change (see https://github.com/ray-project/ray/issues/6130).
    @ray.remote
    def g():
        return nonexistent()

    with pytest.raises(RayTaskError, match="nonexistent"):
        ray.get(g.remote())

    def nonexistent():
        return 1

    # Redefine the function and make sure it succeeds.
    @ray.remote
    def g():
        return nonexistent()

    assert ray.get(g.remote()) == 1

    # Check the same thing but when the redefined function is inside of another
    # task.
    @ray.remote
    def h(i):
        @ray.remote
        def j():
            return i

        return j.remote()

    for i in range(20):
        assert ray.get(ray.get(h.remote(i))) == i


def test_call_matrix(shutdown_only):
    ray.init(object_store_memory=1000 * 1024 * 1024)

    @ray.remote
    class Actor:
        def small_value(self):
            return 0

        def large_value(self):
            return np.zeros(10 * 1024 * 1024)

        def echo(self, x):
            if isinstance(x, list):
                x = ray.get(x[0])
            return x

    @ray.remote
    def small_value():
        return 0

    @ray.remote
    def large_value():
        return np.zeros(10 * 1024 * 1024)

    @ray.remote
    def echo(x):
        if isinstance(x, list):
            x = ray.get(x[0])
        return x

    def check(source_actor, dest_actor, is_large, out_of_band):
        print(
            "CHECKING",
            "actor" if source_actor else "task",
            "to",
            "actor" if dest_actor else "task",
            "large_object" if is_large else "small_object",
            "out_of_band" if out_of_band else "in_band",
        )
        if source_actor:
            a = Actor.remote()
            if is_large:
                x_id = a.large_value.remote()
            else:
                x_id = a.small_value.remote()
        else:
            if is_large:
                x_id = large_value.remote()
            else:
                x_id = small_value.remote()
        if out_of_band:
            x_id = [x_id]
        if dest_actor:
            b = Actor.remote()
            x = ray.get(b.echo.remote(x_id))
        else:
            x = ray.get(echo.remote(x_id))
        if is_large:
            assert isinstance(x, np.ndarray)
        else:
            assert isinstance(x, int)

    for is_large in [False, True]:
        for source_actor in [False, True]:
            for dest_actor in [False, True]:
                for out_of_band in [False, True]:
                    check(source_actor, dest_actor, is_large, out_of_band)


def test_actor_call_order(shutdown_only):
    ray.init(num_cpus=4)

    @ray.remote
    def small_value():
        time.sleep(0.01 * np.random.randint(0, 10))
        return 0

    @ray.remote
    class Actor:
        def __init__(self):
            self.count = 0

        def inc(self, count, dependency):
            assert count == self.count
            self.count += 1
            return count

    a = Actor.remote()
    assert ray.get([a.inc.remote(i, small_value.remote()) for i in range(100)]) == list(
        range(100)
    )


def test_actor_pass_by_ref_order_optimization(shutdown_only):
    ray.init(num_cpus=4)

    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self, x):
            pass

    a = Actor.remote()

    @ray.remote
    def fast_value():
        print("fast value")
        pass

    @ray.remote
    def slow_value():
        print("start sleep")
        time.sleep(30)

    @ray.remote
    def runner(f):
        print("runner", a, f)
        return ray.get(a.f.remote(f.remote()))

    runner.remote(slow_value)
    time.sleep(1)
    x2 = runner.remote(fast_value)
    start = time.time()
    ray.get(x2)
    delta = time.time() - start
    assert delta < 10, "did not skip slow value"


@pytest.mark.parametrize(
    "ray_start_cluster_enabled",
    [
        {
            "num_cpus": 1,
            "num_nodes": 1,
        },
        {
            "num_cpus": 1,
            "num_nodes": 2,
        },
    ],
    indirect=True,
)
def test_call_chain(ray_start_cluster_enabled):
    @ray.remote
    def g(x):
        return x + 1

    x = 0
    for _ in range(100):
        x = g.remote(x)
    assert ray.get(x) == 100


if __name__ == "__main__":
    import pytest

    # Skip test_basic_2_client_mode for now- the test suite is breaking.
    sys.exit(pytest.main(["-v", __file__]))
