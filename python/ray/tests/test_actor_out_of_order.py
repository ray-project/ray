import sys

import ray
import ray.cluster_utils
from ray._private.test_utils import SignalActor


def test_threaded_actor_execute_out_of_order(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        def echo(self, inp):
            print(inp)
            return inp

    actor = SignalActor.remote()

    inp_ref_1 = actor.wait.remote()
    inp_ref_2 = ray.put(2)

    a = A.options(max_concurrency=2).remote()

    a.echo.remote(inp_ref_1)
    out_ref_2 = a.echo.remote(inp_ref_2)

    assert ray.get(out_ref_2, timeout=5) == 2


def test_async_actor_execute_out_of_order(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        async def echo(self, inp):
            print(inp)
            return inp

    actor = SignalActor.remote()

    inp_ref_1 = actor.wait.remote()
    inp_ref_2 = ray.put(2)

    a = A.options(max_concurrency=2).remote()

    a.echo.remote(inp_ref_1)
    out_ref_2 = a.echo.remote(inp_ref_2)

    assert ray.get(out_ref_2, timeout=5) == 2


if __name__ == "__main__":
    import pytest

    # Test suite is timing out. Disable on windows for now.
    sys.exit(pytest.main(["-v", __file__]))
