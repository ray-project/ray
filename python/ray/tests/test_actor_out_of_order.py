import sys

import pytest

import ray
import ray.cluster_utils
from ray._common.test_utils import SignalActor


def test_threaded_actor_allow_out_of_order_execution(shutdown_only):
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


def test_async_actor_allow_out_of_order_execution(shutdown_only):
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


class TestAllowOutOfOrderExecutionValidation:
    @pytest.fixture(scope="class", autouse=True)
    def start_ray_cluster(self):
        ray.init()
        yield
        ray.shutdown()

    def test_options_with_in_order_async_actor_raises_error(self):
        @ray.remote
        class Actor:
            async def method(self):
                pass

        with pytest.raises(ValueError):
            Actor.options(allow_out_of_order_execution=False).remote()

    def test_remote_with_in_order_concurrent_actor_raises_error(self):
        class Actor:
            async def method(self):
                pass

        with pytest.raises(ValueError):
            ray.remote(allow_out_of_order_execution=False)(Actor).remote()

    def test_options_with_in_order_multi_threaded_actor_raises_error(self):
        @ray.remote(max_concurrency=2)
        class Actor:
            pass

        with pytest.raises(ValueError):
            Actor.options(allow_out_of_order_execution=False).remote()

    def test_remote_with_in_order_multi_threaded_actor_raises_error(self):
        class Actor:
            pass

        with pytest.raises(ValueError):
            ray.remote(max_concurrency=2, allow_out_of_order_execution=False)(
                Actor
            ).remote()


if __name__ == "__main__":
    # Test suite is timing out. Disable on windows for now.
    sys.exit(pytest.main(["-sv", __file__]))
