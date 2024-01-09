import asyncio
import logging
from typing import Tuple

import click

from ray import serve
from ray.serve._private.benchmarks.common import run_throughput_benchmark
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

    async def _do_single_batch(self):
        await asyncio.gather(*[self._h.hi.remote() for _ in range(self._batch_size)])

    async def run_benchmark(self) -> Tuple[float, float]:
        return await run_throughput_benchmark(
            fn=self._do_single_batch,
            multiplier=self._batch_size,
            num_trials=self._num_trials,
            trial_runtime=self._trial_runtime,
        )


@click.command(help="Benchmark deployment handle throughput.")
@click.option(
    "--batch-size",
    type=int,
    default=100,
    help="Number of requests to send to downstream deployment in each trial.",
)
@click.option(
    "--num-replicas",
    type=int,
    default=1,
    help="Number of replicas in the downstream deployment.",
)
@click.option(
    "--num-trials",
    type=int,
    default=5,
    help="Number of trials of the benchmark to run.",
)
@click.option(
    "--trial-runtime",
    type=int,
    default=1,
    help="Duration to run each trial of the benchmark for (seconds).",
)
def main(
    batch_size: int,
    num_replicas: int,
    num_trials: int,
    trial_runtime: float,
):
    app = Caller.bind(
        Downstream.options(num_replicas=num_replicas).bind(),
        batch_size=batch_size,
        num_trials=num_trials,
        trial_runtime=trial_runtime,
    )
    h = serve.run(app).options(
        use_new_handle_api=True,
    )

    mean, stddev = h.run_benchmark.remote().result()
    print(
        "DeploymentHandle throughput {}: {} +- {} requests/s".format(
            f"(num_replicas={num_replicas}, batch_size={batch_size})",
            mean,
            stddev,
        )
    )


if __name__ == "__main__":
    main()
