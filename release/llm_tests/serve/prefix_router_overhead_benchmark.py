"""Routing overhead benchmark for PrefixCacheAffinityRouter.

Measures how much routing costs with PrefixCacheAffinityRouter, separate from
any model work: the replicas reply immediately, so routing is the only cost.

Methodology:
- NUM_REPLICAS replicas behind PrefixCacheAffinityRouter with its defaults.
- Requests carry ~1 KiB prompts that share one of NUM_SYSTEM_PROMPTS system
  prompts, so the router's prefix tree has matches to find.
- Closed-loop load: at each concurrency level, that many clients send requests
  back to back for DURATION_S, after a WARMUP_S warmup.
- Two paths, each on a fresh deployment:
  - handle: ``handle.remote()``, as Serve LLM's OpenAI ingress calls LLMServer.
  - direct: ``handle.choose_replica(..., _reserve=False)``, as the
    direct-streaming LLMRouter picks a replica for HAProxy to forward to.

Measures per path and concurrency:
- Throughput (requests or picks per second)
- p50 and p99 latency
- p99 router event loop stall: how much a 1 ms sleep on the event loop that
  routes every DeploymentHandle in the process oversleeps. Blocking calls on
  that loop delay all routing in the process.

Usage (CI):
    python prefix_router_overhead_benchmark.py

Usage (manual):
    python prefix_router_overhead_benchmark.py --smoke-test -o /tmp/results.json
"""

import asyncio
import itertools
import json
import logging
import os
import random
import string
import threading
import time
from collections import Counter
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import click

from ray import serve
from ray.serve._private.router import SingletonThreadRouter
from ray.serve.config import RequestRouterConfig
from ray.serve.handle import DeploymentHandle
from ray.serve.llm.request_router import PrefixCacheAffinityRouter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

APP_NAME = "prefix-router-overhead-benchmark"
PATHS = ["handle", "direct"]

NUM_REPLICAS = 2
NUM_SYSTEM_PROMPTS = 8
NUM_PROMPTS = 4096
PROMPT_CHARS = 1024
SYSTEM_PROMPT_CHARS = 768
CONCURRENCY = [1, 8, 32, 64]
WARMUP_CONCURRENCY = 4
WARMUP_S = 2.0
DURATION_S = 5.0
HEARTBEAT_S = 0.001
# Serve LLM's default max_ongoing_requests: replicas never reject requests.
MAX_ONGOING_REQUESTS = int(1e9)


@serve.deployment(ray_actor_options={"num_cpus": 0})
class NoopLLMServer:
    """Replies immediately with its replica ID."""

    def __init__(self):
        self._replica_id = serve.get_replica_context().replica_id.unique_id

    async def __call__(self, request: SimpleNamespace) -> str:
        return self._replica_id


def build_app():
    return NoopLLMServer.options(
        num_replicas=NUM_REPLICAS,
        max_ongoing_requests=MAX_ONGOING_REQUESTS,
        request_router_config=RequestRouterConfig(
            request_router_class=PrefixCacheAffinityRouter
        ),
    ).bind()


def _random_text(rng: random.Random, n: int) -> str:
    return "".join(rng.choices(string.ascii_lowercase + " ", k=n))


def make_prompts(seed: int) -> List[str]:
    """Prompts that each start with one of a few shared system prompts."""
    rng = random.Random(seed)
    system_prompts = [
        _random_text(rng, SYSTEM_PROMPT_CHARS) for _ in range(NUM_SYSTEM_PROMPTS)
    ]
    return [
        system_prompts[i % NUM_SYSTEM_PROMPTS]
        + _random_text(rng, PROMPT_CHARS - SYSTEM_PROMPT_CHARS)
        for i in range(NUM_PROMPTS)
    ]


def _percentile_ms(values: List[float], q: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return 1000 * ordered[min(len(ordered) - 1, int(q * len(ordered)))]


async def run_level(
    handle: DeploymentHandle,
    path: str,
    concurrency: int,
    duration_s: float,
    prompts: List[str],
) -> Dict[str, Any]:
    """Runs closed-loop clients and returns throughput, latency and loop stalls."""
    stalls: List[float] = []
    stop = threading.Event()

    async def heartbeat():
        while not stop.is_set():
            start = time.perf_counter()
            await asyncio.sleep(HEARTBEAT_S)
            stalls.append(time.perf_counter() - start - HEARTBEAT_S)

    # The loop that routes every DeploymentHandle in this process. It exists once
    # the first request has been sent, which the warmup does.
    router_loop = SingletonThreadRouter._asyncio_loop
    beat = None
    if router_loop is not None:
        beat = asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(heartbeat(), router_loop)
        )

    latencies: List[float] = []
    picks: Counter = Counter()
    next_prompt = itertools.cycle(prompts).__next__
    deadline = time.perf_counter() + duration_s

    async def client():
        while time.perf_counter() < deadline:
            request = SimpleNamespace(prompt=next_prompt())
            start = time.perf_counter()
            if path == "handle":
                replica_id = await handle.remote(request)
            else:
                async with handle.choose_replica(request, _reserve=False) as selection:
                    replica_id = selection.replica_id
            latencies.append(time.perf_counter() - start)
            picks[replica_id] += 1

    start = time.perf_counter()
    await asyncio.gather(*(client() for _ in range(concurrency)))
    elapsed_s = time.perf_counter() - start
    stop.set()
    if beat is not None:
        await beat
    return {
        "requests": len(latencies),
        "throughput_rps": len(latencies) / elapsed_s,
        "latency_p50_ms": _percentile_ms(latencies, 0.50),
        "latency_p99_ms": _percentile_ms(latencies, 0.99),
        "router_loop_stall_p99_ms": _percentile_ms(stalls, 0.99),
        "busiest_replica_share": max(picks.values()) / len(latencies),
    }


def to_perf_metrics(
    path: str, concurrency: int, level: Dict[str, Any]
) -> List[Dict[str, Any]]:
    prefix = f"{path}_c{concurrency}"
    # THROUGHPUT metrics are better when higher, LATENCY metrics when lower.
    metrics = [
        ("throughput_rps", level["throughput_rps"], "THROUGHPUT"),
        ("p50_latency_ms", level["latency_p50_ms"], "LATENCY"),
        ("p99_latency_ms", level["latency_p99_ms"], "LATENCY"),
        ("router_loop_stall_p99_ms", level["router_loop_stall_p99_ms"], "LATENCY"),
    ]
    return [
        {
            "perf_metric_name": f"{prefix}_{name}",
            "perf_metric_value": round(float(value), 4),
            "perf_metric_type": metric_type,
        }
        for name, value, metric_type in metrics
    ]


def run_benchmark(
    paths: List[str],
    concurrency: List[int],
    warmup_s: float,
    duration_s: float,
    seed: int,
) -> Dict[str, Any]:
    prompts = make_prompts(seed)
    perf_metrics: List[Dict[str, Any]] = []
    levels: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for path in paths:
        handle = serve.run(build_app(), name=f"{APP_NAME}-{path}", route_prefix=None)
        try:
            asyncio.run(run_level(handle, path, WARMUP_CONCURRENCY, warmup_s, prompts))
            levels[path] = {}
            for c in concurrency:
                level = asyncio.run(run_level(handle, path, c, duration_s, prompts))
                logger.info(
                    f"{path}, concurrency {c}: {level['throughput_rps']:.0f} req/s, "
                    f"p50 {level['latency_p50_ms']:.2f} ms, "
                    f"p99 {level['latency_p99_ms']:.2f} ms, "
                    f"router loop stall p99 {level['router_loop_stall_p99_ms']:.2f} ms, "
                    f"busiest replica {level['busiest_replica_share']:.0%}"
                )
                levels[path][str(c)] = level
                perf_metrics += to_perf_metrics(path, c, level)
        finally:
            # Also removes the router's prefix tree, so each path starts empty.
            serve.shutdown()
    return {"perf_metrics": perf_metrics, "levels": levels}


def save_test_results(results: Dict[str, Any], output_path: Optional[str]) -> None:
    path = output_path or os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/release_test_out.json"
    )
    with open(path, "wt") as f:
        json.dump(results, f)


@click.command()
@click.option(
    "--output-path",
    "-o",
    type=str,
    default=None,
    help="Where to write results. Default: $TEST_OUTPUT_JSON.",
)
@click.option(
    "--path",
    "-p",
    "paths",
    multiple=True,
    type=click.Choice(PATHS),
    help="Routing paths to benchmark. Repeat the flag for several. Default: both.",
)
@click.option(
    "--concurrency",
    "-c",
    multiple=True,
    type=int,
    help=f"Concurrency levels. Repeat the flag for several. Default: {CONCURRENCY}.",
)
@click.option("--seed", type=int, default=0, show_default=True)
@click.option(
    "--smoke-test",
    is_flag=True,
    help="Run a short version, to check the script.",
)
def main(
    output_path: Optional[str],
    paths: Tuple[str, ...],
    concurrency: Tuple[int, ...],
    seed: int,
    smoke_test: bool,
):
    warmup_s, duration_s = WARMUP_S, DURATION_S
    if smoke_test:
        warmup_s, duration_s = 1.0, 2.0
        concurrency = concurrency or (1, 16)
    paths = list(paths or PATHS)
    concurrency = list(concurrency or CONCURRENCY)
    logger.info(
        f"Running the PrefixCacheAffinityRouter overhead benchmark: paths={paths} "
        f"concurrency={concurrency} warmup_s={warmup_s} duration_s={duration_s}"
    )

    results = run_benchmark(paths, concurrency, warmup_s, duration_s, seed)

    logger.info(f"Perf metrics:\n{json.dumps(results['perf_metrics'], indent=4)}")
    save_test_results(results, output_path)


if __name__ == "__main__":
    main()
