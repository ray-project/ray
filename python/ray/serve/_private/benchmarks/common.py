import inspect
import time
from typing import Callable, Coroutine, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm


class Blackhole:
    def sink(self, o):
        pass


async def collect_profile_events(coro: Coroutine):
    """Collects profiling events using Viztracer"""

    from viztracer import VizTracer

    tracer = VizTracer()
    tracer.start()

    await coro

    tracer.stop()
    tracer.save()


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
    fn: Callable, multiplier: int = 1, num_trials: int = 10, trial_runtime: float = 1
) -> Tuple[float, float]:
    """Returns (mean, stddev)."""
    # Warmup
    start = time.time()
    while time.time() - start < 0.1:
        await fn()

    # Benchmark
    stats = []
    for _ in range(num_trials):
        start = time.perf_counter()
        count = 0
        while time.perf_counter() - start < trial_runtime:
            await fn()
            count += 1
        end = time.perf_counter()
        stats.append(multiplier * count / (end - start))

    return round(np.mean(stats), 2), round(np.std(stats), 2)
