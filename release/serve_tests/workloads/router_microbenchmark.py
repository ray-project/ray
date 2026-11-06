"""Router benchmark: CapacityQueueRouter vs PowerOfTwoChoices.

Compares routers across small (8), medium (32), large (128),
and xlarge (512) replica scales.

Measures per configuration:
- p50 throughput (req/s)
- p50 client end-to-end latency (ms)
- p50 app latency (ms)
- p50 actual child processing latency (ms)
- Per-replica utilization (p25, p50, p75)

Methodology:
- Parent->Child deployment chain where Child simulates work via asyncio.sleep
  with an exponential distribution (mean/cap configurable via CLI).
- Closed-loop load generation: N concurrent users each making sequential
  requests through DeploymentHandle, distributed across remote actors.
- Load level: 100% of theoretical max throughput, which equals to
  num_replicas * max_ongoing_requests.
- Serve access logs are disabled so logging throughput does not dominate
  low-latency microbenchmark results.

Usage (CI):
    python workloads/router_microbenchmark.py

Usage (manual):
    python workloads/router_microbenchmark.py -o /tmp/results.json

Plot results (offline, after downloading CI output):
    python workloads/plot_router_benchmark.py results.json -o /tmp/plots
"""

import asyncio
import json
import logging
import math
import random
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import click
import numpy as np
import ray
from ray import serve
from ray.serve.config import DeploymentActorConfig, RequestRouterConfig
from ray.serve.experimental.capacity_queue import CapacityQueue
from ray.serve.handle import DeploymentHandle
from serve_test_utils import save_test_results

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Load-test configuration
# ---------------------------------------------------------------------------
LOAD_LEVEL = 1.0  # fraction of theoretical max
WARMUP_S = 10.0
DURATION_S = 60.0
THROUGHPUT_WINDOW_S = 5.0  # window size for per-window throughput
MAX_USERS_PER_TASK = 48  # max concurrent users per load-gen task
LOAD_GEN_START_DELAY_S = 5.0

# ---------------------------------------------------------------------------
# Scales and router types
# ---------------------------------------------------------------------------
NUM_REPLICAS = [512, 128, 32, 8]

ROUTER_TYPES = ["pow2", "capacity_queue"]

APP_NAME = "router-benchmark"
LOGGING_CONFIG = {"enable_access_log": False}


# ===================================================================
# Deployments
# ===================================================================


@serve.deployment(
    max_queued_requests=-1,
    graceful_shutdown_timeout_s=0.1,
    graceful_shutdown_wait_loop_s=0.1,
    ray_actor_options={"num_cpus": 1},
)
class BenchmarkChild:
    """Simulates work with variable latency drawn from an exponential distribution."""

    def __init__(
        self,
        simulated_latency_mean_s: float,
        simulated_latency_cap_s: float,
    ):
        self._replica_id = serve.get_replica_context().replica_id.unique_id
        self._mean_s = simulated_latency_mean_s
        self._cap_s = simulated_latency_cap_s

    async def __call__(self) -> dict:
        simulated_latency_s = min(
            random.expovariate(1 / self._mean_s),
            self._cap_s,
        )
        processing_start = time.perf_counter()
        await asyncio.sleep(simulated_latency_s)
        processing_s = time.perf_counter() - processing_start
        return {
            "replica_id": self._replica_id,
            "processing_s": processing_s,
            "simulated_processing_s": simulated_latency_s,
        }


@serve.deployment(
    max_queued_requests=-1,
    graceful_shutdown_timeout_s=0.1,
    graceful_shutdown_wait_loop_s=0.1,
    ray_actor_options={"num_cpus": 1},
)
class BenchmarkParent:
    """Routes requests to the child deployment and returns child replica id."""

    def __init__(self, child: DeploymentHandle):
        self._child = child

    async def __call__(self) -> dict:
        app_start = time.perf_counter()
        resp = await self._child.remote()
        resp["app_latency_s"] = time.perf_counter() - app_start
        return resp


# ===================================================================
# App builders
# ===================================================================


@dataclass
class WorkloadConfig:
    simulated_latency_mean_s: float
    simulated_latency_cap_s: float
    max_ongoing_requests_child: int
    max_ongoing_requests_parent: int


def _build_pow2_app(num_replicas: int, wl: WorkloadConfig):
    """Build app with default Power-of-Two-Choices router."""
    child = BenchmarkChild.options(
        num_replicas=num_replicas,
        max_ongoing_requests=wl.max_ongoing_requests_child,
    ).bind(wl.simulated_latency_mean_s, wl.simulated_latency_cap_s)
    return BenchmarkParent.options(
        num_replicas=num_replicas,
        max_ongoing_requests=wl.max_ongoing_requests_parent,
    ).bind(child)


def _build_capacity_queue_app(num_replicas: int, wl: WorkloadConfig):
    """Build app with CapacityQueueRouter."""
    router_config = RequestRouterConfig(
        request_router_class=(
            "ray.serve.experimental.capacity_queue_router:CapacityQueueRouter"
        ),
        request_router_kwargs={
            "capacity_queue_actor_name": "capacity_queue",
            "max_fault_retries": 3,
        },
        initial_backoff_s=0.05,
        backoff_multiplier=2.0,
        max_backoff_s=1.0,
    )

    def _capacity_queue_actors():
        return [
            DeploymentActorConfig(
                name="capacity_queue",
                actor_class=CapacityQueue,
                init_kwargs={
                    "acquire_timeout_s": 0.5,
                    "token_ttl_s": 5,
                },
                actor_options={"num_cpus": 0},
            ),
        ]

    child = BenchmarkChild.options(
        num_replicas=num_replicas,
        max_ongoing_requests=wl.max_ongoing_requests_child,
        request_router_config=router_config,
        deployment_actors=_capacity_queue_actors(),
    ).bind(wl.simulated_latency_mean_s, wl.simulated_latency_cap_s)
    return BenchmarkParent.options(
        num_replicas=num_replicas,
        max_ongoing_requests=wl.max_ongoing_requests_parent,
        request_router_config=router_config,
        deployment_actors=_capacity_queue_actors(),
    ).bind(child)


def _build_app(router_type: str, num_replicas: int, wl: WorkloadConfig):
    """Build a Parent->Child app with the given router type and scale."""
    if router_type == "pow2":
        return _build_pow2_app(num_replicas, wl)
    elif router_type == "capacity_queue":
        return _build_capacity_queue_app(num_replicas, wl)
    raise ValueError(f"Unknown router type: {router_type}")


# ===================================================================
# Load generation
# ===================================================================


@dataclass
class RequestResult:
    start_time: float
    end_time: float
    latency_ms: float
    app_latency_ms: float  # parent replica -> child replica -> parent replica
    child_replica_id: str
    processing_s: float  # actual child wall-clock time spent in simulated work
    simulated_processing_s: float  # sampled sleep duration requested by the child
    success: bool


@ray.remote(num_cpus=1)
class LoadGenTask:
    """Remote actor that runs a batch of closed-loop users."""

    def __init__(self, app_name: str):
        self._handle = serve.get_deployment_handle("BenchmarkParent", app_name=app_name)

    def ready(self) -> bool:
        return True

    async def run(
        self, num_users: int, warmup_s: float, duration_s: float, start_at: float
    ) -> List[Dict]:
        sleep_s = start_at - time.time()
        if sleep_s > 0:
            await asyncio.sleep(sleep_s)

        start = start_at
        warmup_end = start + warmup_s
        test_end = start + warmup_s + duration_s

        async def user_loop() -> List[Dict]:
            results = []
            while time.time() < test_end:
                req_start = time.time()
                try:
                    resp = await self._handle.remote()
                    req_end = time.time()
                    if req_start >= warmup_end:
                        results.append(
                            {
                                "start_time": req_start,
                                "end_time": req_end,
                                "latency_ms": (req_end - req_start) * 1000,
                                "app_latency_ms": resp["app_latency_s"] * 1000,
                                "child_replica_id": resp["replica_id"],
                                "processing_s": resp["processing_s"],
                                "simulated_processing_s": resp[
                                    "simulated_processing_s"
                                ],
                                "success": True,
                            }
                        )
                except Exception:
                    req_end = time.time()
                    if req_start >= warmup_end:
                        results.append(
                            {
                                "start_time": req_start,
                                "end_time": req_end,
                                "latency_ms": (req_end - req_start) * 1000,
                                "app_latency_ms": 0.0,
                                "child_replica_id": "error",
                                "processing_s": 0.0,
                                "simulated_processing_s": 0.0,
                                "success": False,
                            }
                        )
            return results

        user_results = await asyncio.gather(*[user_loop() for _ in range(num_users)])
        return [r for batch in user_results for r in batch]


async def _run_load_test(
    num_concurrent: int,
    warmup_s: float,
    duration_s: float,
    max_users_per_task: int = MAX_USERS_PER_TASK,
) -> List[RequestResult]:
    """Run a closed-loop load test and return per-request results.

    Distributes *num_concurrent* users across multiple remote
    LoadGenTask actors (up to max_users_per_task users each) to
    avoid bottlenecking the driver's event loop at large scales.
    """
    if max_users_per_task <= 0:
        raise ValueError("max_users_per_task must be positive.")

    num_tasks = max(1, math.ceil(num_concurrent / max_users_per_task))
    base_users = num_concurrent // num_tasks
    remainder = num_concurrent % num_tasks
    users_per_task = [
        base_users + (1 if i < remainder else 0) for i in range(num_tasks)
    ]

    logger.info(
        f"Load test: {num_concurrent} users across {num_tasks} tasks, "
        f"{warmup_s}s warmup + {duration_s}s measurement, "
        f"max_users_per_task={max_users_per_task}"
    )

    tasks = [LoadGenTask.remote(APP_NAME) for _ in range(num_tasks)]
    await asyncio.gather(
        *[asyncio.wrap_future(t.ready.remote().future()) for t in tasks]
    )
    # Use a shared clock edge so autoscaling or actor placement delays do not
    # give each load-gen actor a different measurement window.
    start_at = time.time() + LOAD_GEN_START_DELAY_S
    futures = [
        t.run.remote(n, warmup_s, duration_s, start_at)
        for t, n in zip(tasks, users_per_task)
    ]
    all_dicts = await asyncio.gather(
        *[asyncio.wrap_future(f.future()) for f in futures]
    )

    results = []
    for batch in all_dicts:
        for d in batch:
            results.append(
                RequestResult(
                    start_time=d["start_time"],
                    end_time=d["end_time"],
                    latency_ms=d["latency_ms"],
                    app_latency_ms=d["app_latency_ms"],
                    child_replica_id=d["child_replica_id"],
                    processing_s=d["processing_s"],
                    simulated_processing_s=d["simulated_processing_s"],
                    success=d["success"],
                )
            )
    logger.info(f"Collected {len(results)} results from {num_tasks} tasks")

    # Clean up load-gen actors
    for t in tasks:
        ray.kill(t)

    return results


# ===================================================================
# Metric computation
# ===================================================================


def _compute_throughput_p50(
    results: List[RequestResult],
    window_s: float = THROUGHPUT_WINDOW_S,
) -> float:
    """p50 of per-window throughput (RPS)."""
    successful = [r for r in results if r.success]
    if not successful:
        return 0.0

    min_t = min(r.start_time for r in successful)
    max_t = max(r.end_time for r in successful)
    duration = max_t - min_t
    if duration <= 0:
        return 0.0
    if duration <= window_s:
        return len(successful) / duration

    num_windows = int(duration / window_s)
    window_counts = [0] * num_windows
    for r in successful:
        idx = min(int((r.start_time - min_t) / window_s), num_windows - 1)
        window_counts[idx] += 1

    window_rps = [c / window_s for c in window_counts]
    return float(np.median(window_rps))


def _compute_utilization(
    results: List[RequestResult],
    num_replicas: int,
    duration_s: float,
    max_ongoing_requests_child: int,
) -> List[float]:
    """Per-replica utilization as a list (one value per replica).

    Utilization = fraction of replica slot-time spent processing.
    1.0 means the router has zero overhead (slots always busy).
    Requests that extend past the measurement window are clamped
    so utilization never exceeds 1.0.
    """
    successful = [r for r in results if r.success]
    if not successful:
        return [0.0] * num_replicas

    available_s = duration_s * max_ongoing_requests_child

    # Measurement window: starts at earliest request, spans duration_s
    window_start = min(r.start_time for r in successful)
    window_end = window_start + duration_s

    busy: Dict[str, float] = {}
    for r in successful:
        # Clamp contribution to time remaining in the measurement window
        contribution = min(r.processing_s, max(0.0, window_end - r.start_time))
        busy[r.child_replica_id] = busy.get(r.child_replica_id, 0.0) + contribution

    utilizations = [t / available_s for t in busy.values()]
    utilizations.extend([0.0] * max(0, num_replicas - len(busy)))
    return utilizations


# ===================================================================
# Readiness helpers
# ===================================================================


async def _wait_for_ready(
    handle: DeploymentHandle,
    timeout_s: float = 300.0,
    num_probes: int = 5,
):
    """Block until multiple consecutive probe requests succeed.

    Sends *num_probes* sequential requests to ensure routing tables are
    populated and the deployment is fully warmed up.
    """
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            await handle.remote()
            break
        except Exception:
            await asyncio.sleep(2.0)
    else:
        raise TimeoutError(f"App not ready after {timeout_s}s")

    # Send additional probes to warm routing tables
    for _ in range(num_probes - 1):
        await handle.remote()
    logger.info(f"App ready ({time.time() - start:.1f}s, {num_probes} probes)")


# ===================================================================
# Benchmark runner
# ===================================================================


async def run_router_benchmark(
    workload: WorkloadConfig,
    warmup_s: float = WARMUP_S,
    duration_s: float = DURATION_S,
    num_replicas_list: Optional[List[int]] = None,
    router_types: Optional[List[str]] = None,
    max_users_per_task: int = MAX_USERS_PER_TASK,
) -> Dict:
    """Run the router benchmark and return results dict.

    Returns {"perf_metrics": [...], "utilization_raw": {...}} where
    utilization_raw maps "router_type_replicas" to per-replica values.
    """
    if num_replicas_list is None:
        num_replicas_list = NUM_REPLICAS
    if router_types is None:
        router_types = ROUTER_TYPES

    perf_metrics: List[Dict] = []
    utilization_raw: Dict[str, List[float]] = {}

    for router_type in router_types:
        for num_replicas in num_replicas_list:
            num_concurrent = max(
                1,
                int(num_replicas * workload.max_ongoing_requests_child * LOAD_LEVEL),
            )
            prefix = f"router_{router_type}_{num_replicas}"

            logger.info(
                f"=== {router_type} @ {num_replicas} replicas "
                f"({num_concurrent} users) ==="
            )

            handle = serve.run(
                _build_app(router_type, num_replicas, workload),
                name=APP_NAME,
                logging_config=LOGGING_CONFIG,
            )

            try:
                # Scale readiness probes with replica count so routing
                # tables are fully populated before the load test starts.
                num_probes = max(5, num_replicas // 32)
                await _wait_for_ready(handle, num_probes=num_probes)

                # Scale warmup with replica count
                scaled_warmup = max(warmup_s, warmup_s * (num_replicas / 32))

                results = await _run_load_test(
                    num_concurrent=num_concurrent,
                    warmup_s=scaled_warmup,
                    duration_s=duration_s,
                    max_users_per_task=max_users_per_task,
                )

                successful = [r for r in results if r.success]
                total = len(results)
                failed = total - len(successful)
                if total:
                    logger.info(
                        f"  {prefix}: {total} total, "
                        f"{failed} failed ({failed / total * 100:.1f}%)"
                    )

                # -- throughput --
                tp50 = _compute_throughput_p50(results)
                perf_metrics.append(
                    {
                        "perf_metric_name": f"{prefix}_p50_throughput_rps",
                        "perf_metric_value": round(tp50, 2),
                        "perf_metric_type": "THROUGHPUT",
                    }
                )

                # -- latency --
                if successful:
                    app_latencies = [r.app_latency_ms for r in successful]
                    e2e_latencies = [r.latency_ms for r in successful]
                    processing_latencies = [r.processing_s * 1000 for r in successful]
                    perf_metrics.append(
                        {
                            "perf_metric_name": f"{prefix}_p50_latency_ms",
                            "perf_metric_value": round(
                                float(np.median(e2e_latencies)), 2
                            ),
                            "perf_metric_type": "LATENCY",
                        }
                    )
                    perf_metrics.append(
                        {
                            "perf_metric_name": f"{prefix}_p50_app_latency_ms",
                            "perf_metric_value": round(
                                float(np.median(app_latencies)), 2
                            ),
                            "perf_metric_type": "LATENCY",
                        }
                    )
                    perf_metrics.append(
                        {
                            "perf_metric_name": (f"{prefix}_p50_processing_latency_ms"),
                            "perf_metric_value": round(
                                float(np.median(processing_latencies)), 2
                            ),
                            "perf_metric_type": "LATENCY",
                        }
                    )

                # -- utilization --
                utils = _compute_utilization(
                    results,
                    num_replicas,
                    duration_s,
                    workload.max_ongoing_requests_child,
                )
                utilization_raw[prefix] = [round(u, 4) for u in utils]
                for pct, label in [(25, "p25"), (50, "p50"), (75, "p75")]:
                    perf_metrics.append(
                        {
                            "perf_metric_name": f"{prefix}_{label}_utilization",
                            "perf_metric_value": round(
                                float(np.percentile(utils, pct)), 4
                            ),
                            "perf_metric_type": "THROUGHPUT",
                        }
                    )

            finally:
                await serve.shutdown_async()
                # Let the cluster stabilize before the next deploy,
                # especially important at large scales where actor
                # teardown/creation causes resource churn.
                settle_s = max(5, num_replicas // 32)
                logger.info(f"Settling for {settle_s}s before next config...")
                await asyncio.sleep(settle_s)

    return {"perf_metrics": perf_metrics, "utilization_raw": utilization_raw}


# ===================================================================
# CLI entry point
# ===================================================================


@click.command()
@click.option("--output-path", "-o", type=str, default=None)
@click.option(
    "--num-replicas",
    "-n",
    multiple=True,
    type=int,
    default=NUM_REPLICAS,
    help="Replica counts to benchmark. Default: 512, 128, 32, 8.",
)
@click.option(
    "--router-type",
    "-r",
    multiple=True,
    type=click.Choice(["pow2", "capacity_queue"]),
    required=True,
    help="Routers to benchmark. Repeat flag for multiple.",
)
@click.option(
    "--simulated-latency-mean-s",
    type=float,
    required=True,
    help="Mean of the exponential distribution for simulated child work.",
)
@click.option(
    "--simulated-latency-cap-s",
    type=float,
    required=True,
    help="Cap on simulated child work latency.",
)
@click.option(
    "--max-ongoing-requests-child",
    type=int,
    required=True,
    help="max_ongoing_requests for the child deployment.",
)
@click.option(
    "--max-ongoing-requests-parent",
    type=int,
    required=True,
    help="max_ongoing_requests for the parent deployment.",
)
@click.option(
    "--max-users-per-task",
    type=int,
    default=MAX_USERS_PER_TASK,
    show_default=True,
    help="Max closed-loop users assigned to each load-generator actor.",
)
def main(
    output_path: Optional[str],
    num_replicas: List[int],
    router_type: List[str],
    simulated_latency_mean_s: float,
    simulated_latency_cap_s: float,
    max_ongoing_requests_child: int,
    max_ongoing_requests_parent: int,
    max_users_per_task: int,
):
    workload = WorkloadConfig(
        simulated_latency_mean_s=simulated_latency_mean_s,
        simulated_latency_cap_s=simulated_latency_cap_s,
        max_ongoing_requests_child=max_ongoing_requests_child,
        max_ongoing_requests_parent=max_ongoing_requests_parent,
    )
    logger.info(
        f"Running router benchmark: replicas={list(num_replicas)} "
        f"routers={list(router_type)} workload={workload} "
        f"max_users_per_task={max_users_per_task}"
    )

    results = asyncio.run(
        run_router_benchmark(
            workload=workload,
            warmup_s=WARMUP_S,
            duration_s=DURATION_S,
            num_replicas_list=list(num_replicas),
            router_types=list(router_type),
            max_users_per_task=max_users_per_task,
        )
    )

    logger.info(f"Perf metrics:\n{json.dumps(results['perf_metrics'], indent=4)}")
    save_test_results(results, output_path=output_path)


if __name__ == "__main__":
    main()
