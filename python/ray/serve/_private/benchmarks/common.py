import inspect
import time
from typing import Callable, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm


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
        time.sleep(0.001)
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
    # warmup
    start = time.time()
    while time.time() - start < 0.1:
        await fn()

    # real run
    stats = []
    for _ in range(num_trials):
        start = time.time()
        count = 0
        while time.time() - start < trial_runtime:
            await fn()
            count += 1
        end = time.time()
        stats.append(multiplier * count / (end - start))

    return round(np.mean(stats), 2), round(np.std(stats), 2)
