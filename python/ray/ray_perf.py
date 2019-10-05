"""This is the script for `ray microbenchmark`."""

import time
import numpy as np
import multiprocessing
import ray


@ray.remote
class Actor(object):
    def small_value(self):
        return 0

    def small_value_batch(self, n):
        ray.get([small_value.remote() for _ in range(n)])


@ray.remote
def small_value():
    return 0


@ray.remote
def small_value_batch(n):
    submitted = [small_value.remote() for _ in range(n)]
    ray.get(submitted)
    return 0


def timeit(name, fn, multiplier=1):
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
    ray.init()
    value = ray.put(0)
    arr = np.zeros(100 * 1024 * 1024, dtype=np.int64)

    def get_small():
        ray.get(value)

    timeit("single core get calls", get_small)

    def put_small():
        ray.put(0)

    timeit("single core put calls", put_small)

    def put_large():
        ray.put(arr)

    timeit("single core put gigabytes", put_large, 8 * 0.1)

    @ray.remote
    def do_put_small():
        for _ in range(100):
            ray.put(0)

    def put_multi_small():
        ray.get([do_put_small.remote() for _ in range(10)])

    timeit("multi core put calls", put_multi_small, 1000)

    @ray.remote
    def do_put():
        for _ in range(10):
            ray.put(np.zeros(10 * 1024 * 1024, dtype=np.int64))

    def put_multi():
        ray.get([do_put.remote() for _ in range(10)])

    timeit("multi core put gigabytes", put_multi, 10 * 8 * 0.1)

    def small_task():
        ray.get(small_value.remote())

    timeit("single core tasks sync", small_task)

    def small_task_async():
        ray.get([small_value.remote() for _ in range(1000)])

    timeit("single core tasks async", small_task_async, 1000)

    n = 10000
    m = 4
    actors = [Actor.remote() for _ in range(m)]

    def multi_task():
        submitted = [a.small_value_batch.remote(n) for a in actors]
        ray.get(submitted)

    timeit("multi core tasks async", multi_task, n * m)

    a = Actor.remote()

    def actor_sync():
        ray.get(a.small_value.remote())

    timeit("single core actor calls sync", actor_sync)

    a = Actor.remote()

    def actor_async():
        ray.get([a.small_value.remote() for _ in range(1000)])

    timeit("single core actor calls async", actor_async, 1000)

    n_cpu = multiprocessing.cpu_count() // 2
    a = [Actor.remote() for _ in range(n_cpu)]

    @ray.remote
    def work(actors):
        ray.get([actors[i % n_cpu].small_value.remote() for i in range(n)])

    def actor_multi2():
        ray.get([work.remote(a) for _ in range(m)])

    timeit("multi core actor calls async", actor_multi2, m * n)


if __name__ == "__main__":
    main()
