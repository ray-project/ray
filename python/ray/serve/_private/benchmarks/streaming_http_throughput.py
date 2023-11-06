import time
import asyncio
import logging
from typing import Tuple

import aiohttp
import click
from starlette.responses import StreamingResponse

from ray import serve
from ray.serve._private.benchmarks.common import run_throughput_benchmark
from ray.serve.handle import DeploymentHandle, RayServeHandle

BATCH_SIZE = 1


@serve.deployment(ray_actor_options={"num_cpus": 0})
class Downstream:
    def __init__(self, tokens_per_request: int):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)

        self._tokens_per_request = tokens_per_request

    async def stream(self):
        batches = []
        s = time.time()
        for i in range(self._tokens_per_request):
            batches.append("hi")
            if i % BATCH_SIZE == 0:
                yield batches
                await asyncio.sleep(0)
                batches = []
        e = (time.time() - s)
        print(f"data generation takes {e * 1000} ms. throughput: {self._tokens_per_request / e} / s")

    def __call__(self, *args):
        return StreamingResponse(self.stream())

# yield -> serialize (30us) -> send message (inlined object) to the owner (30us) -> object ref is ready.

@serve.deployment(ray_actor_options={"num_cpus": 0})
class Intermediate:
    def __init__(self, downstream: RayServeHandle):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)

        self._h: DeploymentHandle = downstream.options(
            stream=True,
            use_new_handle_api=True,
        )

    async def stream(self):
        gen = self._h.stream.remote()
        # print("wait until downstream is finished")
        # await asyncio.sleep(10)
        s = time.time()
        total_elapsed = 0
        total_tokens = 0
        # while True:
        #     try:
        #         # anext_time = time.time()
        #         tokens = await gen.__anext__()
        #         # print(f"anext took {(time.time() - anext_time) * 1000} ms")
        #         # await gen._obj_ref_gen._generator_ref
        #         ss = time.time()
        #         # yield tokens
        #         for token in tokens:
        #             total_tokens += 1
        #             yield token
        #         total_elapsed += (time.time() - ss) * 1000
        #     except StopAsyncIteration:
        #         break
        async for tokens in gen:
            ss = time.time()
            for token in tokens:
                total_tokens += 1
                yield token
            total_elapsed += (time.time() - ss) * 1000
            # print(f"inner yield takes {(time.time() - ss) * 1000} ms")
        e = (time.time() - s)
        print(f"yield takes {e * 1000} ms, inner elapse: {total_elapsed}, throughput: {total_tokens / e} / s")

    def __call__(self, *args):
        return StreamingResponse(self.stream())


async def _consume_single_stream():
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        async with session.get("http://localhost:8000") as r:
            async for line in r.content:
                pass


async def run_benchmark(
    tokens_per_request: int,
    batch_size: int,
    num_trials: int,
    trial_runtime: float,
) -> Tuple[float, float]:
    async def _do_single_batch():
        await asyncio.gather(*[_consume_single_stream() for _ in range(batch_size)])

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
    app = Downstream.options(num_replicas=num_replicas).bind(tokens_per_request)
    if use_intermediate_deployment:
        app = Intermediate.bind(app)

    serve.run(app)

    mean, stddev = asyncio.new_event_loop().run_until_complete(
        run_benchmark(
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
