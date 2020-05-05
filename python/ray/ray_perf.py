"""This is the script for `ray microbenchmark`."""

import asyncio
import os
import time
import numpy as np
import multiprocessing
import ray

# Only run tests matching this filter pattern.
filter_pattern = os.environ.get("TESTS_TO_RUN", "")


@ray.remote(num_cpus=0)
class Actor:
    def small_value(self):
        return b"ok"

    def small_value_arg(self, x):
        return b"ok"

    def small_value_batch(self, n):
        ray.get([small_value.remote() for _ in range(n)])


@ray.remote
class AsyncActor:
    async def small_value(self):
        return b"ok"

    async def small_value_with_arg(self, x):
        return b"ok"

    async def small_value_batch(self, n):
        await asyncio.wait([small_value.remote() for _ in range(n)])


@ray.remote(num_cpus=0)
class Client:
    def __init__(self, servers):
        if not isinstance(servers, list):
            servers = [servers]
        self.servers = servers

    def small_value_batch(self, n):
        results = []
        for s in self.servers:
            results.extend([s.small_value.remote() for _ in range(n)])
        ray.get(results)

    def small_value_batch_arg(self, n):
        x = ray.put(0)
        results = []
        for s in self.servers:
            results.extend([s.small_value_arg.remote(x) for _ in range(n)])
        ray.get(results)


@ray.remote
def small_value():
    return b"ok"


@ray.remote
def small_value_batch(n):
    submitted = [small_value.remote() for _ in range(n)]
    ray.get(submitted)
    return 0


def timeit(name, fn, multiplier=1):
    if filter_pattern not in name:
        return
    # warmup
    start = time.time()
    while time.time() - start < 1:
        fn()
    # real run
    stats = []
    for _ in range(4):
        start = time.time()
        count = 0
        while time.time() - start < 2:
            fn()
            count += 1
        end = time.time()
        stats.append(multiplier * count / (end - start))
    print(name, "per second", round(np.mean(stats), 2), "+-",
          round(np.std(stats), 2))


def main():
    print("Tip: set TESTS_TO_RUN='pattern' to run a subset of benchmarks")
    ray.init()

    value = ray.put(0)
    arr = np.zeros(100 * 1024 * 1024, dtype=np.int64)

    def get_small():
        ray.get(value)

    timeit("single client get calls", get_small)

    def put_small():
        ray.put(0)

    timeit("single client put calls", put_small)

    def put_large():
        ray.put(arr)

    timeit("single client put gigabytes", put_large, 8 * 0.1)

    @ray.remote
    def do_put_small():
        for _ in range(100):
            ray.put(0)

    def put_multi_small():
        ray.get([do_put_small.remote() for _ in range(10)])

    timeit("multi client put calls", put_multi_small, 1000)

    @ray.remote
    def do_put():
        for _ in range(10):
            ray.put(np.zeros(10 * 1024 * 1024, dtype=np.int64))

    def put_multi():
        ray.get([do_put.remote() for _ in range(10)])

    timeit("multi client put gigabytes", put_multi, 10 * 8 * 0.1)

    def small_task():
        ray.get(small_value.remote())

    timeit("single client tasks sync", small_task)

    def small_task_async():
        ray.get([small_value.remote() for _ in range(1000)])

    timeit("single client tasks async", small_task_async, 1000)

    n = 10000
    m = 4
    actors = [Actor.remote() for _ in range(m)]

    def multi_task():
        submitted = [a.small_value_batch.remote(n) for a in actors]
        ray.get(submitted)

    timeit("multi client tasks async", multi_task, n * m)

    a = Actor.remote()

    def actor_sync():
        ray.get(a.small_value.remote())

    timeit("1:1 actor calls sync", actor_sync)

    a = Actor.remote()

    def actor_async():
        ray.get([a.small_value.remote() for _ in range(1000)])

    timeit("1:1 actor calls async", actor_async, 1000)

    a = Actor.options(max_concurrency=16).remote()

    def actor_concurrent():
        ray.get([a.small_value.remote() for _ in range(1000)])

    timeit("1:1 actor calls concurrent", actor_concurrent, 1000)

    n = 5000
    n_cpu = multiprocessing.cpu_count() // 2
    actors = [Actor._remote() for _ in range(n_cpu)]
    client = Client.remote(actors)

    def actor_async_direct():
        ray.get(client.small_value_batch.remote(n))

    timeit("1:n actor calls async", actor_async_direct, n * len(actors))

    n_cpu = multiprocessing.cpu_count() // 2
    a = [Actor.remote() for _ in range(n_cpu)]

    @ray.remote
    def work(actors):
        ray.get([actors[i % n_cpu].small_value.remote() for i in range(n)])

    def actor_multi2():
        ray.get([work.remote(a) for _ in range(m)])

    timeit("n:n actor calls async", actor_multi2, m * n)

    n = 1000
    actors = [Actor._remote() for _ in range(n_cpu)]
    clients = [Client.remote(a) for a in actors]

    def actor_multi2_direct_arg():
        ray.get([c.small_value_batch_arg.remote(n) for c in clients])

    timeit("n:n actor calls with arg async", actor_multi2_direct_arg,
           n * len(clients))

    a = AsyncActor.remote()

    def actor_sync():
        ray.get(a.small_value.remote())

    timeit("1:1 async-actor calls sync", actor_sync)

    a = AsyncActor.remote()

    def async_actor():
        ray.get([a.small_value.remote() for _ in range(1000)])

    timeit("1:1 async-actor calls async", async_actor, 1000)

    a = AsyncActor.remote()

    def async_actor():
        ray.get([a.small_value_with_arg.remote(i) for i in range(1000)])

    timeit("1:1 async-actor calls with args async", async_actor, 1000)

    n = 5000
    n_cpu = multiprocessing.cpu_count() // 2
    actors = [AsyncActor.remote() for _ in range(n_cpu)]
    client = Client.remote(actors)

    def async_actor_async():
        ray.get(client.small_value_batch.remote(n))

    timeit("1:n async-actor calls async", async_actor_async, n * len(actors))

    n = 5000
    m = 4
    n_cpu = multiprocessing.cpu_count() // 2
    a = [AsyncActor.remote() for _ in range(n_cpu)]

    @ray.remote
    def async_actor_work(actors):
        ray.get([actors[i % n_cpu].small_value.remote() for i in range(n)])

    def async_actor_multi():
        ray.get([async_actor_work.remote(a) for _ in range(m)])

    timeit("n:n async-actor calls async", async_actor_multi, m * n)


if __name__ == "__main__":
    main()
