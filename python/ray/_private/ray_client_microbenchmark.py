import inspect
import logging
import sys

from ray.util.client.ray_client_helpers import ray_start_client_server

from ray._private.ray_microbenchmark_helpers import timeit


def benchmark_get_calls(ray):
    value = ray.put(0)

    def get_small():
        ray.get(value)

    timeit("client: get calls", get_small)


def benchmark_put_calls(ray):
    def put_small():
        ray.put(0)

    timeit("client: put calls", put_small)


def benchmark_remote_put_calls(ray):
    @ray.remote
    def do_put_small():
        for _ in range(100):
            ray.put(0)

    def put_multi_small():
        ray.get([do_put_small.remote() for _ in range(10)])

    timeit("client: remote put calls", put_multi_small, 1000)


def benchmark_simple_actor(ray):
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

    timeit("client: 1:1 actor calls sync", actor_sync)

    def actor_async():
        ray.get([a.small_value.remote() for _ in range(1000)])

    timeit("client: 1:1 actor calls async", actor_async, 1000)

    a = Actor.options(max_concurrency=16).remote()

    def actor_concurrent():
        ray.get([a.small_value.remote() for _ in range(1000)])

    timeit("client: 1:1 actor calls concurrent", actor_concurrent, 1000)


def main():
    ray_config = {
        "_system_config": {
            "put_small_object_in_memory_store": True
        },
        "logging_level": logging.WARNING
    }

    def ray_connect_handler(job_config=None):
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
            obj(ray)


if __name__ == "__main__":
    main()
