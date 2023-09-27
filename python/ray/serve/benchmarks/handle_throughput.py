import argparse
import asyncio
import logging

from ray import serve
from ray.serve.benchmarks.microbenchmark import timeit
from ray.serve.handle import DeploymentHandle, RayServeHandle


@serve.deployment(ray_actor_options={"num_cpus": 0})
class Downstream:
    def __init__(self):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)

    def hi(self) -> bytes:
        return b"hi"


@serve.deployment
class Caller:
    def __init__(
        self,
        downstream: RayServeHandle,
        *,
        batch_size: int,
        num_trials: int,
        trial_runtime: float,
    ):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)

        self._h: DeploymentHandle = downstream.options(use_new_handle_api=True)
        self._batch_size = batch_size
        self._num_trials = num_trials
        self._trial_runtime = trial_runtime

    async def do_single_batch(self):
        await asyncio.gather(*[self._h.hi.remote() for _ in range(self._batch_size)])

    async def run_benchmark(self):
        return await timeit(
            fn=self.do_single_batch,
            multiplier=self._batch_size,
            num_trials=self._num_trials,
            trial_runtime=self._trial_runtime,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark deployment handle throughput."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of requests to send to downstream deployment in each trial.",
    )
    parser.add_argument(
        "--num-replicas",
        type=int,
        default=10,
        help="Number of replicas in the downstream deployment.",
    )
    parser.add_argument(
        "--num-trials",
        type=float,
        default=5,
        help="How many trials of the benchmark to run.",
    )
    parser.add_argument(
        "--trial-runtime",
        type=float,
        default=1,
        help="How long to run each trial of the benchmark for (seconds).",
    )

    args = parser.parse_args()

    app = Caller.bind(
        Downstream.options(num_replicas=args.num_replicas).bind(),
        batch_size=args.batch_size,
        num_trials=args.num_trials,
        trial_runtime=args.trial_runtime,
    )
    h = serve.run(app).options(
        use_new_handle_api=True,
    )

    mean, stddev = h.run_benchmark.remote().result()
    print(
        "DeploymentHandle throughput {}: {} +- {} requests/s".format(
            f"(num_replicas={args.num_replicas}, batch_size={args.batch_size})",
            mean,
            stddev,
        )
    )
