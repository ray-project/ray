import asyncio
import time
from typing import Dict

from ray.util import metrics

from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)

_METRICS_LOOP_INTERVAL = 5  # 5 seconds
EVENT_LOOP_LATENCY_HISTOGRAM_BOUNDARIES = [
    0.05,
    0.1,
    0.15,
    0.20,
    0.25,
    0.5,
    0.75,
    1.0,
    1.5,
    2.0,
    3.0,
    5.0,
    10.0,
    15.0,
    20.0,
    30.0,
    45.0,
    60.0,
    90.0,
    120.0,
    150.0,
    180.0,
    300.0,
    600.0,
]


def setup_event_loop_monitoring(
    loop: asyncio.AbstractEventLoop,
    scheduling_latency: metrics.Histogram,
    iterations: metrics.Counter,
    tasks: metrics.Gauge,
    tags: Dict[str, str],
) -> asyncio.Task:
    return asyncio.create_task(
        _run_monitoring_loop(
            loop,
            schedule_latency=scheduling_latency,
            iterations=iterations,
            task_gauge=tasks,
            tags=tags,
        )
    )


async def _run_monitoring_loop(
    loop: asyncio.AbstractEventLoop,
    schedule_latency: metrics.Histogram,
    iterations: metrics.Counter,
    task_gauge: metrics.Gauge,
    tags: Dict[str, str],
) -> None:
    while loop.is_running():
        iterations.inc(1, tags)
        num_tasks = len(asyncio.all_tasks())
        task_gauge.set(num_tasks, tags)
        yield_time = time.monotonic()
        await asyncio.sleep(_METRICS_LOOP_INTERVAL)
        elapsed_time = time.monotonic() - yield_time

        # Historically, Ray's implementation of histograms are extremely finicky
        # with non-positive values (https://github.com/ray-project/ray/issues/26698).
        # Technically it shouldn't be possible for this to be negative, add the
        # max just to be safe.
        latency = max(0.0, elapsed_time - _METRICS_LOOP_INTERVAL)
        schedule_latency.observe(latency, tags)
