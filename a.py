import time
import asyncio
import logging
from typing import Tuple
import ray

import aiohttp
import click
from starlette.responses import StreamingResponse

from ray import serve
from ray.serve._private.benchmarks.common import run_throughput_benchmark
from ray.serve.handle import DeploymentHandle, RayServeHandle

BATCH_SIZE = 1


@ray.remote(num_cpus=0)
class Downstream:
    def __init__(self, tokens_per_request: int):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)

        self._tokens_per_request = tokens_per_request

    def stream(self, i):
        batches = []
        s = time.time()
        for i in range(self._tokens_per_request):
            batches.append("hi")
            if i % BATCH_SIZE == 0:
                ss = time.time()
                yield batches
                print(f"yield takes {(time.time() - ss) * 1000} ms")
                batches = []
        e = (time.time() - s)
        print(f"data generation takes {e * 1000} ms. throughput: {self._tokens_per_request / e} / s")


@ray.remote(num_cpus=0)
class Intermediate:
    def __init__(self, downstream):
        self._h = downstream

    async def stream(self, i):
        gen = self._h.stream.options(
            num_returns="streaming"
        ).remote(i)
        print("wait until downstream is finished")
        # await asyncio.sleep(10)
        s = time.time()
        total_elapsed = 0
        total_tokens = 0
        from viztracer import VizTracer
        while True:
            try:
                anext_time = time.time()
                if total_tokens == 10:
                    with VizTracer(output_file="/tmp/a.json", log_async=True, log_gc=True,) as tracer:
                        tokens = await gen.__anext__()
                        tokens = await tokens
                else:
                    tokens = await gen.__anext__()
                    tokens = await tokens
                print(f"id {i} anext took {(time.time() - anext_time) * 1000} ms")
                # await gen._obj_ref_gen._generator_ref
                ss = time.time()
                for token in tokens:
                    total_tokens += 1
                    yield token
                total_elapsed += (time.time() - ss) * 1000
            except StopAsyncIteration:
                break
        e = (time.time() - s)
        print(f"id {i} yield takes {e * 1000} ms, inner elapse: {total_elapsed}, throughput: {total_tokens / e} / s")


async def _consume_single_stream(h, i):
    # async for line in h.stream.options(num_returns="streaming").remote(i):
    #     # print(ray.get(line))
    #     pass
    gen = h.stream.options(num_returns="streaming").remote(i)
    await gen._generator_ref


async def run_benchmark(
    h,
    tokens_per_request: int,
    batch_size: int,
    num_trials: int,
    trial_runtime: float,
) -> Tuple[float, float]:
    async def _do_single_batch():
        s = time.time()
        await asyncio.gather(*[_consume_single_stream(h, i) for i in range(batch_size)])
        print(f"Took {(batch_size * 1000) / ((time.time() - s))} tokens/s to iterate at the driver.")

    return await run_throughput_benchmark(
        fn=_do_single_batch,
        multiplier=batch_size * tokens_per_request,
        num_trials=num_trials,
        trial_runtime=trial_runtime,
    )


@click.command(help="Benchmark streaming HTTP throughput.")
@click.option(
    "--tokens-per-request",
    type=int,
    default=1000,
    help="Number of requests to send to downstream deployment in each trial.",
)
@click.option(
    "--batch-size",
    type=int,
    default=10,
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
@click.option(
    "--use-intermediate-deployment",
    is_flag=True,
    default=False,
    help="Whether to run an intermediate deployment proxying the requests.",
)
def main(
    tokens_per_request: int,
    batch_size: int,
    num_replicas: int,
    num_trials: int,
    trial_runtime: float,
    use_intermediate_deployment: bool,
):
    app = Downstream.remote(tokens_per_request)
    if use_intermediate_deployment:
        app = Intermediate.remote(app)

    mean, stddev = asyncio.new_event_loop().run_until_complete(
        run_benchmark(
            app,
            tokens_per_request,
            batch_size,
            num_trials,
            trial_runtime,
        )
    )
    print(
        "HTTP streaming throughput {}: {} +- {} tokens/s".format(
            f"(num_replicas={num_replicas}, "
            f"tokens_per_request={tokens_per_request}, "
            f"batch_size={batch_size}, "
            f"use_intermediate_deployment={use_intermediate_deployment})",
            mean,
            stddev,
        )
    )


if __name__ == "__main__":
    main()
