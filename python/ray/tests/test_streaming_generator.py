import pytest
import numpy as np
import sys
import time

import ray
from ray._private.test_utils import wait_for_condition


def test_generator_basic(shutdown_only):
    ray.init(num_cpus=1)

    """Basic cases"""

    @ray.remote
    def f():
        for i in range(5):
            yield i

    gen = f.options(num_returns="streaming").remote()
    i = 0
    for ref in gen:
        print(ray.get(ref))
        assert i == ray.get(ref)
        del ref
        i += 1

    """Exceptions"""

    @ray.remote
    def f():
        for i in range(5):
            if i == 2:
                raise ValueError
            yield i

    gen = f.options(num_returns="streaming").remote()
    ray.get(next(gen))
    ray.get(next(gen))
    with pytest.raises(ray.exceptions.RayTaskError) as e:
        ray.get(next(gen))
    print(str(e.value))
    with pytest.raises(StopIteration):
        ray.get(next(gen))
    with pytest.raises(StopIteration):
        ray.get(next(gen))

    """Generator Task failure"""

    @ray.remote
    class A:
        def getpid(self):
            import os

            return os.getpid()

        def f(self):
            for i in range(5):
                time.sleep(0.1)
                yield i

    a = A.remote()
    i = 0
    gen = a.f.options(num_returns="streaming").remote()
    i = 0
    for ref in gen:
        if i == 2:
            ray.kill(a)
        if i == 3:
            with pytest.raises(ray.exceptions.RayActorError) as e:
                ray.get(ref)
            assert "The actor is dead because it was killed by `ray.kill`" in str(
                e.value
            )
            break
        assert i == ray.get(ref)
        del ref
        i += 1
    for _ in range(10):
        with pytest.raises(StopIteration):
            next(gen)

    """Retry exceptions"""
    # TODO(sang): Enable it once retry is supported.
    # @ray.remote
    # class Actor:
    #     def __init__(self):
    #         self.should_kill = True

    #     def should_kill(self):
    #         return self.should_kill

    #     async def set(self, wait_s):
    #         await asyncio.sleep(wait_s)
    #         self.should_kill = False

    # @ray.remote(retry_exceptions=[ValueError], max_retries=10)
    # def f(a):
    #     for i in range(5):
    #         should_kill = ray.get(a.should_kill.remote())
    #         if i == 3 and should_kill:
    #             raise ValueError
    #         yield i

    # a = Actor.remote()
    # gen = f.options(num_returns="streaming").remote(a)
    # assert ray.get(next(gen)) == 0
    # assert ray.get(next(gen)) == 1
    # assert ray.get(next(gen)) == 2
    # a.set.remote(3)
    # assert ray.get(next(gen)) == 3
    # assert ray.get(next(gen)) == 4
    # with pytest.raises(StopIteration):
    #     ray.get(next(gen))

    """Cancel"""

    @ray.remote
    def f():
        for i in range(5):
            time.sleep(5)
            yield i

    gen = f.options(num_returns="streaming").remote()
    assert ray.get(next(gen)) == 0
    ray.cancel(gen)
    with pytest.raises(ray.exceptions.RayTaskError) as e:
        assert ray.get(next(gen)) == 1
    assert "was cancelled" in str(e.value)
    with pytest.raises(StopIteration):
        next(gen)


@pytest.mark.parametrize("use_actors", [False, True])
@pytest.mark.parametrize("store_in_plasma", [False, True])
def test_generator_streaming(shutdown_only, use_actors, store_in_plasma):
    """Verify the generator is working in a streaming fashion."""
    ray.init()
    remote_generator_fn = None
    if use_actors:

        @ray.remote
        class Generator:
            def __init__(self):
                pass

            def generator(self, num_returns, store_in_plasma):
                for i in range(num_returns):
                    if store_in_plasma:
                        yield np.ones(1_000_000, dtype=np.int8) * i
                    else:
                        yield [i]

        g = Generator.remote()
        remote_generator_fn = g.generator
    else:

        @ray.remote(max_retries=0)
        def generator(num_returns, store_in_plasma):
            for i in range(num_returns):
                if store_in_plasma:
                    yield np.ones(1_000_000, dtype=np.int8) * i
                else:
                    yield [i]

        remote_generator_fn = generator

    """Verify num_returns="streaming" is streaming"""
    gen = remote_generator_fn.options(num_returns="streaming").remote(
        3, store_in_plasma
    )
    i = 0
    for ref in gen:
        id = ref.hex()
        if store_in_plasma:
            expected = np.ones(1_000_000, dtype=np.int8) * i
            assert np.array_equal(ray.get(ref), expected)
        else:
            expected = [i]
            assert ray.get(ref) == expected

        del ref
        from ray.experimental.state.api import list_objects

        wait_for_condition(
            lambda: len(list_objects(filters=[("object_id", "=", id)])) == 0
        )
        i += 1


def test_generator_dist_chain(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, object_store_memory=1 * 1024 * 1024 * 1024)
    ray.init()
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)

    @ray.remote
    class ChainActor:
        def __init__(self, child=None):
            self.child = child

        def get_data(self):
            if not self.child:
                for _ in range(10):
                    time.sleep(0.1)
                    yield np.ones(5 * 1024 * 1024)
            else:
                for data in self.child.get_data.options(
                    num_returns="streaming"
                ).remote():
                    yield ray.get(data)

    chain_actor = ChainActor.remote()
    chain_actor_2 = ChainActor.remote(chain_actor)
    chain_actor_3 = ChainActor.remote(chain_actor_2)
    chain_actor_4 = ChainActor.remote(chain_actor_3)

    for ref in chain_actor_4.get_data.options(num_returns="streaming").remote():
        assert np.array_equal(np.ones(5 * 1024 * 1024), ray.get(ref))
        del ref


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
