import numpy as np
import os
import pytest

import ray


@pytest.fixture(params=[1, 4])
def ray_start_sharded(request):
    num_redis_shards = request.param

    if os.environ.get("RAY_USE_NEW_GCS") == "on":
        num_redis_shards = 1
        # For now, RAY_USE_NEW_GCS supports 1 shard, and credis supports
        # 1-node chain for that shard only.

    # Start the Ray processes.
    ray.init(
        object_store_memory=int(0.5 * 10**9),
        num_cpus=10,
        num_redis_shards=num_redis_shards,
        redis_max_memory=10**7)

    yield None

    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_submitting_many_tasks(ray_start_sharded):
    @ray.remote
    def f(x):
        return 1

    def g(n):
        x = 1
        for i in range(n):
            x = f.remote(x)
        return x

    ray.get([g(100) for _ in range(100)])
    assert ray.services.remaining_processes_alive()


def test_submitting_many_actors_to_one(ray_start_sharded):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def ping(self):
            return

    @ray.remote
    class Worker:
        def __init__(self, actor):
            self.actor = actor

        def ping(self):
            return ray.get(self.actor.ping.remote())

    a = Actor.remote()
    workers = [Worker.remote(a) for _ in range(10)]
    for _ in range(10):
        out = ray.get([w.ping.remote() for w in workers])
        assert out == [None for _ in workers]


def test_getting_and_putting(ray_start_sharded):
    for n in range(8):
        x = np.zeros(10**n)

        for _ in range(100):
            ray.put(x)

        x_id = ray.put(x)
        for _ in range(1000):
            ray.get(x_id)

    assert ray.services.remaining_processes_alive()


def test_getting_many_objects(ray_start_sharded):
    @ray.remote
    def f():
        return 1

    n = 10**4  # TODO(pcm): replace by 10 ** 5 once this is faster.
    lst = ray.get([f.remote() for _ in range(n)])
    assert lst == n * [1]

    assert ray.services.remaining_processes_alive()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
