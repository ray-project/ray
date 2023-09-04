import asyncio
import pytest
import numpy as np
import sys
import time

from collections import Counter

import ray
from ray._raylet import StreamingObjectRefGenerator


def test_threaded_actor_generator(shutdown_only):
    ray.init()

    @ray.remote(max_concurrency=10)
    class Actor:
        def f(self):
            for i in range(30):
                time.sleep(0.1)
                yield np.ones(1024 * 1024) * i

    @ray.remote(max_concurrency=20)
    class AsyncActor:
        async def f(self):
            for i in range(30):
                await asyncio.sleep(0.1)
                yield np.ones(1024 * 1024) * i

    async def main():
        a = Actor.remote()
        asy = AsyncActor.remote()

        async def run():
            i = 0
            async for ref in a.f.options(num_returns="streaming").remote():
                val = ray.get(ref)
                print(val)
                print(ref)
                assert np.array_equal(val, np.ones(1024 * 1024) * i)
                i += 1
                del ref

        async def run2():
            i = 0
            async for ref in asy.f.options(num_returns="streaming").remote():
                val = await ref
                print(ref)
                print(val)
                assert np.array_equal(val, np.ones(1024 * 1024) * i), ref
                i += 1
                del ref

        coroutines = [run() for _ in range(10)]
        coroutines = [run2() for _ in range(20)]

        await asyncio.gather(*coroutines)

    asyncio.run(main())


def test_generator_dist_gather(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, object_store_memory=1 * 1024 * 1024 * 1024)
    ray.init()
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self, child=None):
            self.child = child

        def get_data(self):
            for _ in range(10):
                time.sleep(0.1)
                yield np.ones(5 * 1024 * 1024)

    async def all_gather():
        actor = Actor.remote()
        async for ref in actor.get_data.options(num_returns="streaming").remote():
            val = await ref
            assert np.array_equal(np.ones(5 * 1024 * 1024), val)
            del ref

    async def main():
        await asyncio.gather(all_gather(), all_gather(), all_gather(), all_gather())

    asyncio.run(main())
    summary = ray._private.internal_api.memory_summary(stats_only=True)
    print(summary)


def test_generator_wait(shutdown_only):
    """
    Make sure the generator works with ray.wait.
    """
    ray.init(num_cpus=8)

    @ray.remote
    def f(sleep_time):
        for i in range(2):
            time.sleep(sleep_time)
            yield i

    @ray.remote
    def g(sleep_time):
        time.sleep(sleep_time)
        return 10

    gen = f.options(num_returns="streaming").remote(1)

    """
    Test basic cases.
    """
    for expected_rval in [0, 1]:
        s = time.time()
        r, ur = ray.wait([gen], num_returns=1)
        print(time.time() - s)
        assert len(r) == 1
        assert ray.get(next(r[0])) == expected_rval
        assert len(ur) == 0

    # Should raise a stop iteration.
    for _ in range(3):
        s = time.time()
        r, ur = ray.wait([gen], num_returns=1)
        print(time.time() - s)
        assert len(r) == 1
        with pytest.raises(StopIteration):
            assert next(r[0]) == 0
        assert len(ur) == 0

    gen = f.options(num_returns="streaming").remote(0)
    # Wait until the generator task finishes
    ray.get(gen._generator_ref)
    for i in range(2):
        r, ur = ray.wait([gen], timeout=0)
        assert len(r) == 1
        assert len(ur) == 0
        assert ray.get(next(r[0])) == i

    """
    Test the case ref is mixed with regular object ref.
    """
    gen = f.options(num_returns="streaming").remote(0)
    ref = g.remote(3)
    ready, unready = [], [gen, ref]
    result_set = set()
    while unready:
        ready, unready = ray.wait(unready)
        print(ready, unready)
        assert len(ready) == 1
        for r in ready:
            if isinstance(r, StreamingObjectRefGenerator):
                try:
                    ref = next(r)
                    print(ref)
                    print(ray.get(ref))
                    result_set.add(ray.get(ref))
                except StopIteration:
                    pass
                else:
                    unready.append(r)
            else:
                result_set.add(ray.get(r))

    assert result_set == {0, 1, 10}

    """
    Test timeout.
    """
    gen = f.options(num_returns="streaming").remote(3)
    ref = g.remote(1)
    ready, unready = ray.wait([gen, ref], timeout=2)
    assert len(ready) == 1
    assert len(unready) == 1

    """
    Test num_returns
    """
    gen = f.options(num_returns="streaming").remote(1)
    ref = g.remote(1)
    ready, unready = ray.wait([ref, gen], num_returns=2)
    assert len(ready) == 2
    assert len(unready) == 0


def test_generator_wait_e2e(shutdown_only):
    ray.init(num_cpus=8)

    @ray.remote
    def f(sleep_time):
        for i in range(2):
            time.sleep(sleep_time)
            yield i

    @ray.remote
    def g(sleep_time):
        time.sleep(sleep_time)
        return 10

    gen = [f.options(num_returns="streaming").remote(1) for _ in range(4)]
    ref = [g.remote(2) for _ in range(4)]
    ready, unready = [], [*gen, *ref]
    result = []
    start = time.time()
    while unready:
        ready, unready = ray.wait(unready, num_returns=len(unready), timeout=0.1)
        for r in ready:
            if isinstance(r, StreamingObjectRefGenerator):
                try:
                    ref = next(r)
                    result.append(ray.get(ref))
                except StopIteration:
                    pass
                else:
                    unready.append(r)
            else:
                result.append(ray.get(r))
    elapsed = time.time() - start
    assert elapsed < 4
    assert 2 < elapsed

    assert len(result) == 12
    result = Counter(result)
    assert result[0] == 4
    assert result[1] == 4
    assert result[10] == 4


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
