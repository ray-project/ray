import inspect
import logging
import numpy as np
import sys

from ray.util.client.ray_client_helpers import ray_start_client_server

from ray._private.ray_microbenchmark_helpers import timeit


def benchmark_get_calls(ray, results):
    value = ray.put(0)

    def get_small():
        ray.get(value)

    results += timeit("client: get calls", get_small)


def benchmark_tasks_and_get_batch(ray, results):
    @ray.remote
    def small_value():
        return b"ok"

    def small_value_batch():
        submitted = [small_value.remote() for _ in range(1000)]
        ray.get(submitted)
        return 0

    results += timeit("client: tasks and get batch", small_value_batch)


def benchmark_put_calls(ray, results):
    def put_small():
        ray.put(0)

    results += timeit("client: put calls", put_small)


def benchmark_remote_put_calls(ray, results):
    @ray.remote
    def do_put_small():
        for _ in range(100):
            ray.put(0)

    def put_multi_small():
        ray.get([do_put_small.remote() for _ in range(10)])

    results += timeit("client: tasks and put batch", put_multi_small, 1000)


def benchmark_put_large(ray, results):
    arr = np.zeros(100 * 1024 * 1024, dtype=np.int64)

    def put_large():
        ray.put(arr)

    results += timeit("client: put gigabytes", put_large, 8 * 0.1)


def benchmark_simple_actor(ray, results):
    @ray.remote(num_cpus=0)
    class Actor:
        def small_value(self):
            return b"ok"

        def small_value_arg(self, x):
            return b"ok"

        def small_value_batch(self, n):
            ray.get([self.small_value.remote() for _ in range(n)])

    a = Actor.remote()

    def actor_sync():
        ray.get(a.small_value.remote())

    results += timeit("client: 1:1 actor calls sync", actor_sync)

    def actor_async():
        ray.get([a.small_value.remote() for _ in range(1000)])

    results += timeit("client: 1:1 actor calls async", actor_async, 1000)

    a = Actor.options(max_concurrency=16).remote()

    def actor_concurrent():
        ray.get([a.small_value.remote() for _ in range(1000)])

    results += timeit("client: 1:1 actor calls concurrent", actor_concurrent,
                      1000)


def main(results=None):
    results = results or []

    ray_config = {"logging_level": logging.WARNING}

    def ray_connect_handler(job_config=None, **ray_init_kwargs):
        from ray._private.client_mode_hook import disable_client_hook
        with disable_client_hook():
            import ray as real_ray
            if not real_ray.is_initialized():
                real_ray.init(**ray_config)

    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if not name.startswith("benchmark_"):
            continue
        with ray_start_client_server(
                ray_connect_handler=ray_connect_handler) as ray:
            obj(ray, results)

    return results


if __name__ == "__main__":
    main()
