"""LoRA routing benchmark for PrefixCacheAffinityRouter.

Sends LoRA traffic through PrefixCacheAffinityRouter to fake Ray Serve LLM
replicas and measures where the router places adapters and requests.

The replicas run no model. They mimic vLLM with LoRA behind Serve's real model
multiplexing (``@serve.multiplexed``):
- A replica keeps at most ADAPTERS_PER_REPLICA adapters loaded, Serve LLM's
  default. Its first load of an adapter downloads it (``download_s``); later
  loads hit the disk cache (``reload_s``). It loads one adapter at a time, as
  Serve does.
- At most ``max_loras`` distinct adapters run at once, as in vLLM.
- A request holds one of ``slots`` batch slots for prefill, which is shorter on a
  prefix cache hit (keyed by adapter, like vLLM's prefix cache), plus decode.
- Replicas never reject requests, like Serve LLM's default max_ongoing_requests.

Load: open-loop Poisson traffic, the same for every run with the same seed. Each
request picks an adapter and one of that adapter's system prompts. Workloads:
- uniform: every adapter equally popular, each with its own system prompts.
- zipf: adapter popularity proportional to 1 / rank^1.1 (the top adapter gets
  28% of requests).
- shared_prompt: uniform, but all adapters share the same system prompts.

Measures per workload, for requests sent in the cold window (adapters still
being downloaded), the steady window after it, and the last ``settled_s`` seconds:
- Share of requests whose adapter was already loaded on arrival
- Share of requests that hit the prefix cache
- Adapter evictions
- Share of requests on the busiest replica
- Latency percentiles, and the time from sending a request to its arrival at a
  replica (routing)

Usage (CI):
    python prefix_router_lora_benchmark.py

Usage (manual):
    python prefix_router_lora_benchmark.py --smoke-test -o /tmp/results.json
"""

import asyncio
import dataclasses
import json
import logging
import os
import random
import string
import time
from collections import Counter, OrderedDict, defaultdict
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import click

from ray import serve
from ray.serve.config import RequestRouterConfig
from ray.serve.handle import DeploymentHandle
from ray.serve.llm.request_router import PrefixCacheAffinityRouter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

APP_NAME = "prefix-router-lora-benchmark"
WORKLOADS = ["uniform", "zipf", "shared_prompt"]
PHASES = ("cold", "steady")

# Serve LLM's defaults: max_num_adapters_per_replica, and a max_ongoing_requests
# so high that replicas never reject requests.
ADAPTERS_PER_REPLICA = 16
MAX_ONGOING_REQUESTS = int(1e9)

# Counters each response reports from its replica.
REPLICA_COUNTERS = ("downloads", "reloads", "evictions", "lora_swaps")


@dataclass
class BenchmarkConfig:
    num_replicas: int = 4
    num_adapters: int = 32
    prompts_per_adapter: int = 2
    prompt_chars: int = 1024
    system_prompt_chars: int = 768
    zipf_s: float = 1.1
    rate: float = 40.0  # requests per second
    warmup_s: float = 60.0  # the cold window
    duration_s: float = 120.0  # the steady window
    settled_s: float = 60.0
    drain_timeout_s: float = 120.0
    timeline_bucket_s: float = 30.0
    seed: int = 0
    # The fake engine.
    download_s: float = 2.0
    reload_s: float = 0.05
    max_loras: int = 4
    lora_swap_s: float = 0.02
    slots: int = 32
    prefix_cache_entries: int = 32
    prefill_miss_s: float = 0.10
    prefill_hit_s: float = 0.03
    decode_s: float = 1.0


SMOKE_TEST_OVERRIDES = dict(
    num_adapters=8,
    rate=20.0,
    warmup_s=10.0,
    duration_s=20.0,
    settled_s=10.0,
    download_s=1.0,
)


# ===================================================================
# Fake LLM replica
# ===================================================================


class LoadedAdapter:
    """What the replica's multiplexed loader returns for one adapter."""

    def __init__(self, adapter_id: str, on_evict):
        self.adapter_id = adapter_id
        self._on_evict = on_evict
        self._evicted = False

    def __del__(self):
        # Serve calls this when it evicts the adapter, and garbage collection may
        # call it again, so count it once.
        if not self._evicted:
            self._evicted = True
            self._on_evict(self.adapter_id)


class LoraSlots:
    """Runs requests of at most ``max_loras`` distinct adapters at once, like vLLM.

    An adapter keeps its slot while idle until another adapter needs it.
    """

    def __init__(self, max_loras: int, swap_s: float):
        self._max_loras = max_loras
        self._swap_s = swap_s
        # Adapter -> number of running requests, least recently used first.
        self._users: "OrderedDict[str, int]" = OrderedDict()
        self._changed = asyncio.Condition()
        self.swaps = 0

    async def acquire(self, adapter_id: str) -> None:
        async with self._changed:
            while True:
                if adapter_id in self._users:
                    self._users[adapter_id] += 1
                    self._users.move_to_end(adapter_id)
                    return
                if len(self._users) < self._max_loras:
                    break
                idle = next((a for a, n in self._users.items() if n == 0), None)
                if idle is not None:
                    del self._users[idle]
                    break
                await self._changed.wait()
            self._users[adapter_id] = 1
            self.swaps += 1
        await asyncio.sleep(self._swap_s)

    async def release(self, adapter_id: str) -> None:
        async with self._changed:
            self._users[adapter_id] -= 1
            self._changed.notify_all()


@serve.deployment(ray_actor_options={"num_cpus": 0})
class FakeLoraServer:
    """An LLM replica serving LoRA adapters, with timers in place of a model."""

    def __init__(self, config: BenchmarkConfig):
        self._config = config
        self._batch_slots = asyncio.Semaphore(config.slots)
        self._lora_slots = LoraSlots(config.max_loras, config.lora_swap_s)
        # (adapter, system prompt) pairs in the prefix cache, least recently used
        # first.
        self._prefix_cache: "OrderedDict[Tuple[str, str], None]" = OrderedDict()
        self._loaded = set()  # In Serve's multiplexed model cache.
        self._downloaded = set()
        self._counts = Counter()
        self._replica_id = serve.get_replica_context().replica_id.unique_id

    def _on_evict(self, adapter_id: str):
        self._loaded.discard(adapter_id)
        self._counts["evictions"] += 1

    @serve.multiplexed(max_num_models_per_replica=ADAPTERS_PER_REPLICA)
    async def get_adapter(self, adapter_id: str) -> LoadedAdapter:
        if adapter_id in self._downloaded:
            self._counts["reloads"] += 1
            await asyncio.sleep(self._config.reload_s)
        else:
            self._counts["downloads"] += 1
            await asyncio.sleep(self._config.download_s)
            self._downloaded.add(adapter_id)
        self._loaded.add(adapter_id)
        return LoadedAdapter(adapter_id, self._on_evict)

    async def __call__(self, request: SimpleNamespace) -> Dict[str, Any]:
        # Wall clock, to compare with the driver's on the same node.
        arrived_at = time.time()
        adapter_id = serve.get_multiplexed_model_id()
        adapter_ready = adapter_id in self._loaded
        await self.get_adapter(adapter_id)
        await self._lora_slots.acquire(adapter_id)
        try:
            async with self._batch_slots:
                # vLLM's prefix cache keys blocks by LoRA adapter, so a prefix
                # computed under one adapter doesn't serve another.
                key = (adapter_id, request.system_prompt_id)
                prefix_hit = key in self._prefix_cache
                if prefix_hit:
                    self._prefix_cache.move_to_end(key)
                else:
                    self._prefix_cache[key] = None
                    if len(self._prefix_cache) > self._config.prefix_cache_entries:
                        self._prefix_cache.popitem(last=False)
                await asyncio.sleep(
                    self._config.prefill_hit_s
                    if prefix_hit
                    else self._config.prefill_miss_s
                )
                await asyncio.sleep(self._config.decode_s)
        finally:
            await self._lora_slots.release(adapter_id)
        return {
            "replica_id": self._replica_id,
            "arrived_at": arrived_at,
            "adapter_ready": adapter_ready,
            "prefix_hit": prefix_hit,
            **{c: self._counts[c] for c in ("downloads", "reloads", "evictions")},
            "lora_swaps": self._lora_slots.swaps,
        }


def build_app(config: BenchmarkConfig, router_kwargs: Dict[str, Any]):
    return FakeLoraServer.options(
        num_replicas=config.num_replicas,
        max_ongoing_requests=MAX_ONGOING_REQUESTS,
        request_router_config=RequestRouterConfig(
            request_router_class=PrefixCacheAffinityRouter,
            request_router_kwargs=router_kwargs,
        ),
    ).bind(config)


# ===================================================================
# Load generation
# ===================================================================


def _random_text(rng: random.Random, n: int) -> str:
    return "".join(rng.choices(string.ascii_lowercase + " ", k=n))


class Workload:
    """Picks an adapter and one of its system prompts for each request."""

    def __init__(self, name: str, config: BenchmarkConfig):
        rng = random.Random(config.seed)
        self.adapters = [f"adapter-{i}" for i in range(config.num_adapters)]
        zipf_s = config.zipf_s if name == "zipf" else 0.0
        weights = [1.0 / (rank + 1) ** zipf_s for rank in range(config.num_adapters)]
        self.weights = [w / sum(weights) for w in weights]
        shared = [
            _random_text(rng, config.system_prompt_chars)
            for _ in range(config.prompts_per_adapter)
        ]
        self.system_prompts = {
            adapter: [
                (f"shared-{i}", shared[i])
                if name == "shared_prompt"
                else (f"{adapter}-{i}", _random_text(rng, config.system_prompt_chars))
                for i in range(config.prompts_per_adapter)
            ]
            for adapter in self.adapters
        }
        self.suffix_chars = config.prompt_chars - config.system_prompt_chars

    def sample(self, rng: random.Random) -> Tuple[str, str, str]:
        """Returns an adapter, a system prompt ID and a prompt."""
        adapter = rng.choices(self.adapters, self.weights)[0]
        system_prompt_id, system_prompt = rng.choice(self.system_prompts[adapter])
        prompt = system_prompt + _random_text(rng, self.suffix_chars)
        return adapter, system_prompt_id, prompt


async def run_load(
    handle: DeploymentHandle, workload: Workload, config: BenchmarkConfig
) -> Tuple[List[Dict[str, Any]], Counter]:
    """Sends open-loop Poisson traffic.

    Returns the completed requests and the number of requests sent per phase.
    """
    rng = random.Random(config.seed + 1)
    records: List[Dict[str, Any]] = []
    sent: Counter = Counter()
    tasks: List[asyncio.Task] = []
    start = time.perf_counter()
    steady_from = start + config.warmup_s
    stop_at = steady_from + config.duration_s

    async def send(adapter_id: str, system_prompt_id: str, prompt: str, phase: str):
        sent_at, sent_at_wall = time.perf_counter(), time.time()
        record = await handle.options(multiplexed_model_id=adapter_id).remote(
            SimpleNamespace(prompt=prompt, system_prompt_id=system_prompt_id)
        )
        record.update(
            latency_s=time.perf_counter() - sent_at,
            routing_s=record.pop("arrived_at") - sent_at_wall,
            sent_s=sent_at - start,
            phase=phase,
            adapter_id=adapter_id,
        )
        records.append(record)

    next_send = start
    while time.perf_counter() < stop_at:
        phase = "cold" if time.perf_counter() < steady_from else "steady"
        sent[phase] += 1
        tasks.append(asyncio.ensure_future(send(*workload.sample(rng), phase)))
        next_send += rng.expovariate(config.rate)
        await asyncio.sleep(max(0.0, next_send - time.perf_counter()))
    done, pending = await asyncio.wait(tasks, timeout=config.drain_timeout_s)
    for task in pending:
        task.cancel()
    # Check every request, including those that finished while traffic was still
    # being sent, so a failed request raises its error.
    for task in done:
        task.result()
    return records, sent


# ===================================================================
# Metrics
# ===================================================================


def _percentile_ms(values: List[float], q: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return 1000 * ordered[min(len(ordered) - 1, int(q * len(ordered)))]


def summarize(
    records: List[Dict[str, Any]], sent: Counter, config: BenchmarkConfig
) -> Dict[str, Any]:
    # Responses carry their replica's running counters. A replica's count for a
    # phase is its highest value in that phase minus its highest value before.
    peak = defaultdict(Counter)
    for r in records:
        for c in REPLICA_COUNTERS:
            key = (r["phase"], r["replica_id"])
            peak[key][c] = max(peak[key][c], r[c])
    replica_ids = {r["replica_id"] for r in records}

    summary: Dict[str, Any] = {}
    for i, phase in enumerate(PHASES):
        rs = [r for r in records if r["phase"] == phase]
        n = len(rs)
        if not n:
            raise RuntimeError(f"No request sent in the {phase} window completed.")
        summary[phase] = {"sent": sent[phase], "completed": n}
        counts = Counter()
        for replica_id in replica_ids:
            before = Counter()
            for earlier in PHASES[:i]:
                before |= peak[earlier, replica_id]
            now = peak[phase, replica_id] | before
            for c in REPLICA_COUNTERS:
                counts[c] += now[c] - before[c]
        per_replica = Counter(r["replica_id"] for r in rs)
        placements = {(r["replica_id"], r["adapter_id"]) for r in rs}
        latencies = [r["latency_s"] for r in rs]
        summary[phase].update(
            adapter_ready_ratio=sum(r["adapter_ready"] for r in rs) / n,
            prefix_hit_ratio=sum(r["prefix_hit"] for r in rs) / n,
            replicas_per_adapter=len(placements) / len({r["adapter_id"] for r in rs}),
            busiest_replica_share=max(per_replica.values()) / n,
            latency_p50_ms=_percentile_ms(latencies, 0.50),
            latency_p99_ms=_percentile_ms(latencies, 0.99),
            routing_p99_ms=_percentile_ms([r["routing_s"] for r in rs], 0.99),
            **counts,
        )

    settled_from = config.warmup_s + config.duration_s - config.settled_s
    settled = [r["latency_s"] for r in records if r["sent_s"] >= settled_from]
    summary["settled"] = {
        "from_s": settled_from,
        "completed": len(settled),
        "latency_p50_ms": _percentile_ms(settled, 0.50),
        "latency_p99_ms": _percentile_ms(settled, 0.99),
    }

    # Latency by send time, to show when the cold start ends.
    buckets = defaultdict(list)
    for r in records:
        buckets[int(r["sent_s"] // config.timeline_bucket_s)].append(r["latency_s"])
    summary["timeline"] = [
        {
            "from_s": b * config.timeline_bucket_s,
            "completed": len(buckets[b]),
            "latency_p50_ms": _percentile_ms(buckets[b], 0.50),
            "latency_p99_ms": _percentile_ms(buckets[b], 0.99),
        }
        for b in sorted(buckets)
    ]
    return summary


def to_perf_metrics(workload: str, summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    steady, cold, settled = summary["steady"], summary["cold"], summary["settled"]
    # THROUGHPUT metrics are better when higher, LATENCY metrics when lower.
    metrics = [
        ("steady_adapter_ready_ratio", steady["adapter_ready_ratio"], "THROUGHPUT"),
        ("steady_prefix_hit_ratio", steady["prefix_hit_ratio"], "THROUGHPUT"),
        ("steady_evictions", steady["evictions"], "LATENCY"),
        ("steady_busiest_replica_share", steady["busiest_replica_share"], "LATENCY"),
        ("steady_routing_p99_ms", steady["routing_p99_ms"], "LATENCY"),
        ("cold_p99_latency_ms", cold["latency_p99_ms"], "LATENCY"),
        ("settled_p50_latency_ms", settled["latency_p50_ms"], "LATENCY"),
        ("settled_p99_latency_ms", settled["latency_p99_ms"], "LATENCY"),
    ]
    return [
        {
            "perf_metric_name": f"{workload}_{name}",
            "perf_metric_value": round(float(value), 4),
            "perf_metric_type": metric_type,
        }
        for name, value, metric_type in metrics
    ]


def log_summary(workload: str, summary: Dict[str, Any]) -> None:
    steady, cold, settled = summary["steady"], summary["cold"], summary["settled"]
    logger.info(
        f"{workload}: adapter ready {steady['adapter_ready_ratio']:.1%}, "
        f"prefix hits {steady['prefix_hit_ratio']:.1%}, "
        f"evictions {steady['evictions']}, "
        f"replicas/adapter {steady['replicas_per_adapter']:.2f}, "
        f"busiest replica {steady['busiest_replica_share']:.1%}, "
        f"cold p99 {cold['latency_p99_ms'] / 1000:.1f} s, "
        f"settled p50/p99 {settled['latency_p50_ms'] / 1000:.1f}/"
        f"{settled['latency_p99_ms'] / 1000:.1f} s"
    )
    timeline = ", ".join(
        f"{b['from_s']:.0f}s {b['latency_p99_ms'] / 1000:.1f}"
        for b in summary["timeline"]
    )
    logger.info(f"{workload}: p99 latency (s) by send time: {timeline}")


# ===================================================================
# Main
# ===================================================================


def run_benchmark(
    config: BenchmarkConfig, workloads: List[str], router_kwargs: Dict[str, Any]
) -> Dict[str, Any]:
    perf_metrics: List[Dict[str, Any]] = []
    summaries: Dict[str, Dict[str, Any]] = {}
    for name in workloads:
        handle = serve.run(
            build_app(config, router_kwargs),
            name=f"{APP_NAME}-{name}",
            route_prefix=None,
        )
        try:
            records, sent = asyncio.run(
                run_load(handle, Workload(name, config), config)
            )
        finally:
            # Also removes the router's prefix tree, so each workload starts empty.
            serve.shutdown()
        summary = summarize(records, sent, config)
        log_summary(name, summary)
        summaries[name] = summary
        perf_metrics += to_perf_metrics(name, summary)
    return {"perf_metrics": perf_metrics, "summaries": summaries}


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
    "--workload",
    "-w",
    "workloads",
    multiple=True,
    type=click.Choice(WORKLOADS),
    help="Workloads to run. Repeat the flag for several. Default: all.",
)
@click.option(
    "--multiplex-spill-threshold",
    type=float,
    default=None,
    help="PrefixCacheAffinityRouter's multiplex_spill_threshold. Default: the "
    "router's default.",
)
@click.option("--seed", type=int, default=0, show_default=True)
@click.option(
    "--smoke-test",
    is_flag=True,
    help="Run a short version with fewer adapters, to check the script.",
)
def main(
    output_path: Optional[str],
    workloads: Tuple[str, ...],
    multiplex_spill_threshold: Optional[float],
    seed: int,
    smoke_test: bool,
):
    config = BenchmarkConfig(seed=seed)
    if smoke_test:
        config = dataclasses.replace(config, **SMOKE_TEST_OVERRIDES)
        workloads = workloads or ("uniform",)
    router_kwargs = {}
    if multiplex_spill_threshold is not None:
        router_kwargs["multiplex_spill_threshold"] = multiplex_spill_threshold
    logger.info(
        f"Running the PrefixCacheAffinityRouter LoRA benchmark: config={config} "
        f"workloads={list(workloads or WORKLOADS)} router_kwargs={router_kwargs}"
    )

    results = run_benchmark(config, list(workloads or WORKLOADS), router_kwargs)

    logger.info(f"Perf metrics:\n{json.dumps(results['perf_metrics'], indent=4)}")
    save_test_results(results, output_path)

    # A request that never completes is a routing bug, not a slow benchmark.
    unfinished = {
        f"{name} {phase}": summary[phase]["sent"] - summary[phase]["completed"]
        for name, summary in results["summaries"].items()
        for phase in PHASES
        if summary[phase]["sent"] > summary[phase]["completed"]
    }
    if unfinished:
        raise RuntimeError(f"Requests didn't complete: {unfinished}")


if __name__ == "__main__":
    main()
