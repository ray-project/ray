import asyncio
from typing import Tuple

import click

import ray
from ray.actor import ActorHandle
from ray.serve._private.benchmarks.common import run_throughput_benchmark


class Blackhole:
    def sink(self, o):
        pass


@ray.remote
class SyncDownstream:
    def __init__(self, tokens_per_request: int):
        self._tokens_per_request = tokens_per_request

    def stream(self):
        for i in range(self._tokens_per_request):
            yield "hi"


@ray.remote
class AsyncDownstream:
    def __init__(self, tokens_per_request: int):
        self._tokens_per_request = tokens_per_request

    async def stream(self):
        for i in range(self._tokens_per_request):
            # await asyncio.sleep(0)
            yield "hi"


@ray.remote
class Caller(Blackhole):
    def __init__(
        self,
        downstream: ActorHandle,
        *,
        tokens_per_request: int,
        batch_size: int,
        num_trials: int,
        trial_runtime: float,
    ):
        self._h: ActorHandle = downstream
        self._tokens_per_request = tokens_per_request
        self._batch_size = batch_size
        self._num_trials = num_trials
        self._trial_runtime = trial_runtime

    async def _consume_single_stream(self):
        async for ref in self._h.stream.options(num_returns="streaming").remote():
            bs = ray.get(ref)
            self.sink(bs)

    async def _do_single_batch(self):
        await asyncio.gather(
            *[self._consume_single_stream() for _ in range(self._batch_size)]
        )

    async def run_benchmark(self) -> Tuple[float, float]:
        return await run_throughput_benchmark(
            fn=self._do_single_batch,
            multiplier=self._batch_size * self._tokens_per_request,
            num_trials=self._num_trials,
            trial_runtime=self._trial_runtime,
        )


@click.command(help="Benchmark streaming deployment handle throughput.")
@click.option(
    "--tokens-per-request",
    type=int,
    default=1000,
    help="Number of tokens (per request) to stream from downstream deployment",
)
@click.option(
    "--batch-size",
    type=int,
    default=10,
    help="Number of requests to send to downstream deployment in each batch.",
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
    default=5,
    help="Duration to run each trial of the benchmark for (seconds).",
)
@click.option(
    "--mode",
    type=str,
    default="sync",
    help="Controls mode of the streaming generation (either 'sync' or 'async')",
)
def main(
    tokens_per_request: int,
    batch_size: int,
    num_replicas: int,
    num_trials: int,
    trial_runtime: float,
    mode: str,
):
    if mode == "sync":
        downstream_class = SyncDownstream
    elif mode == "async":
        downstream_class = AsyncDownstream
    else:
        raise NotImplementedError(f"'{mode}' is not supported")

    h = Caller.remote(
        downstream_class.remote(
            tokens_per_request=tokens_per_request,
        ),
        tokens_per_request=tokens_per_request,
        batch_size=batch_size,
        num_trials=num_trials,
        trial_runtime=trial_runtime,
    )

    mean, stddev = ray.get(h.run_benchmark.remote())
    print(
        "Core Actors streaming throughput ({}) {}: {} +- {} tokens/s".format(
            mode.upper(),
            f"(num_replicas={num_replicas}, "
            f"tokens_per_request={tokens_per_request}, "
            f"batch_size={batch_size})",
            mean,
            stddev,
        )
    )


if __name__ == "__main__":
    main()
