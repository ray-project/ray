import asyncio
import inspect
import logging
import random
import string
import time
from functools import partial
from typing import Any, Callable, Coroutine, List, Optional, Tuple

import aiohttp
import aiohttp.client_exceptions
import grpc
import numpy as np
import pandas as pd
from starlette.responses import StreamingResponse
from tqdm import tqdm

from ray import serve
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.handle import DeploymentHandle


async def run_latency_benchmark(
    f: Callable, num_requests: int, *, num_warmup_requests: int = 100
) -> pd.Series:
    if inspect.iscoroutinefunction(f):
        to_call = f
    else:

        async def to_call():
            f()

    latencies = []
    for i in tqdm(range(num_requests + num_warmup_requests)):
        start = time.perf_counter()
        await to_call()
        end = time.perf_counter()

        # Don't include warm-up requests.
        if i >= num_warmup_requests:
            latencies.append(1000 * (end - start))

    return pd.Series(latencies)


async def run_throughput_benchmark(
    fn: Callable[[], List[float]],
    multiplier: int = 1,
    num_trials: int = 10,
    trial_runtime: float = 1,
) -> Tuple[float, float, pd.Series]:
    """Benchmarks throughput of a function.

    Args:
        fn: The function to benchmark. If this returns anything, it must
            return a list of latencies.
        multiplier: The number of requests or tokens (or whatever unit
            is appropriate for this throughput benchmark) that is
            completed in one call to `fn`.
        num_trials: The number of trials to run.
        trial_runtime: How long each trial should run for. During the
            duration of one trial, `fn` will be repeatedly called.

    Returns (mean, stddev, latencies).
    """
    # Warmup
    start = time.time()
    while time.time() - start < 0.1:
        await fn()

    # Benchmark
    stats = []
    latencies = []
    for _ in tqdm(range(num_trials)):
        start = time.perf_counter()
        count = 0
        while time.perf_counter() - start < trial_runtime:
            res = await fn()
            if res:
                latencies.extend(res)

            count += 1
        end = time.perf_counter()
        stats.append(multiplier * count / (end - start))

    return round(np.mean(stats), 2), round(np.std(stats), 2), pd.Series(latencies)


async def do_single_http_batch(
    *,
    batch_size: int = 100,
    url: str = "http://localhost:8000",
    stream: bool = False,
) -> List[float]:
    """Sends a batch of http requests and returns e2e latencies."""

    # By default, aiohttp limits the number of client connections to 100.
    # We need to use TCPConnector to configure the limit if batch size
    # is greater than 100.
    connector = aiohttp.TCPConnector(limit=batch_size)
    async with aiohttp.ClientSession(
        connector=connector, raise_for_status=True
    ) as session:

        async def do_query():
            start = time.perf_counter()
            try:
                if stream:
                    async with session.get(url) as r:
                        async for chunk, _ in r.content.iter_chunks():
                            pass
                else:
                    await session.get(url)
            except aiohttp.client_exceptions.ClientConnectionError:
                pass

            end = time.perf_counter()
            return 1000 * (end - start)

        return await asyncio.gather(*[do_query() for _ in range(batch_size)])


async def do_single_grpc_batch(
    *, batch_size: int = 100, target: str = "localhost:9000"
):
    channel = grpc.aio.insecure_channel(target)
    stub = serve_pb2_grpc.RayServeBenchmarkServiceStub(channel)
    payload = serve_pb2.StringData(data="")

    async def do_query():
        start = time.perf_counter()

        await stub.grpc_call(payload)

        end = time.perf_counter()
        return 1000 * (end - start)

    return await asyncio.gather(*[do_query() for _ in range(batch_size)])


async def collect_profile_events(coro: Coroutine):
    """Collects profiling events using Viztracer"""

    from viztracer import VizTracer

    tracer = VizTracer()
    tracer.start()

    await coro

    tracer.stop()
    tracer.save()


def generate_payload(size: int = 100, chars=string.ascii_uppercase + string.digits):
    return "".join(random.choice(chars) for _ in range(size))


class Blackhole:
    def sink(self, o):
        pass


@serve.deployment
class Noop:
    def __init__(self):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)

    def __call__(self, *args, **kwargs):
        return b""


@serve.deployment
class Streamer:
    def __init__(self, tokens_per_request: int, inter_token_delay_ms: int = 10):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)
        self._tokens_per_request = tokens_per_request
        self._inter_token_delay_s = inter_token_delay_ms / 1000

    async def stream(self):
        for _ in range(self._tokens_per_request):
            await asyncio.sleep(self._inter_token_delay_s)
            yield b"hi"

    async def __call__(self):
        return StreamingResponse(self.stream())


@serve.deployment
class IntermediateRouter:
    def __init__(self, handle: DeploymentHandle):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)
        self._handle = handle.options(stream=True)

    async def stream(self):
        async for token in self._handle.stream.remote():
            yield token

    def __call__(self):
        return StreamingResponse(self.stream())


@serve.deployment
class Benchmarker:
    def __init__(
        self,
        handle: DeploymentHandle,
        stream: bool = False,
    ):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)
        self._handle = handle.options(stream=stream)
        self._stream = stream

    async def do_single_request(self, payload: Any = None) -> float:
        """Completes a single unary request. Returns e2e latency in ms."""
        start = time.perf_counter()

        if payload is None:
            await self._handle.remote()
        else:
            await self._handle.remote(payload)

        end = time.perf_counter()
        return 1000 * (end - start)

    async def _do_single_stream(self) -> float:
        """Consumes a single streaming request. Returns e2e latency in ms."""
        start = time.perf_counter()

        async for r in self._handle.stream.remote():
            pass

        end = time.perf_counter()
        return 1000 * (end - start)

    async def _do_single_batch(self, batch_size: int) -> List[float]:
        if self._stream:
            return await asyncio.gather(
                *[self._do_single_stream() for _ in range(batch_size)]
            )
        else:
            return await asyncio.gather(
                *[self.do_single_request() for _ in range(batch_size)]
            )

    async def run_latency_benchmark(
        self, *, num_requests: int, payload: Any = None
    ) -> pd.Series:
        async def f():
            await self.do_single_request(payload)

        return await run_latency_benchmark(f, num_requests=num_requests)

    async def run_throughput_benchmark(
        self,
        *,
        batch_size: int,
        num_trials: int,
        trial_runtime: float,
        tokens_per_request: Optional[float] = None,
    ) -> Tuple[float, float]:
        if self._stream:
            assert tokens_per_request
            multiplier = tokens_per_request * batch_size
        else:
            multiplier = batch_size

        return await run_throughput_benchmark(
            fn=partial(
                self._do_single_batch,
                batch_size=batch_size,
            ),
            multiplier=multiplier,
            num_trials=num_trials,
            trial_runtime=trial_runtime,
        )
