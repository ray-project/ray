import asyncio
import inspect
import logging
import random
import string
import time
from functools import partial
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

import aiohttp
import aiohttp.client_exceptions
import grpc
import numpy as np
import pandas as pd
from starlette.responses import StreamingResponse
from tqdm import tqdm

import ray
from ray import serve
from ray._common.test_utils import SignalActor as _SignalActor
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
                async with session.get(url) as r:
                    if stream:
                        async for chunk, _ in r.content.iter_chunks():
                            pass
                    else:
                        # Read the response to ensure it's consumed
                        await r.read()
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
class ModelComp:
    def __init__(self, child):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)
        self._child = child

    async def __call__(self, *args, **kwargs):
        return await self._child.remote()


@serve.deployment
class GrpcDeployment:
    def __init__(self):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)

    async def grpc_call(self, user_message):
        return serve_pb2.ModelOutput(output=9)

    async def call_with_string(self, user_message):
        return serve_pb2.ModelOutput(output=9)


@serve.deployment
class GrpcModelComp:
    def __init__(self, child):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)
        self._child = child

    async def grpc_call(self, user_message):
        await self._child.remote()
        return serve_pb2.ModelOutput(output=9)

    async def call_with_string(self, user_message):
        await self._child.remote()
        return serve_pb2.ModelOutput(output=9)


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


# =============================================================================
# Controller Benchmark
# =============================================================================
# See https://github.com/ray-project/ray/issues/60680 for more details.

CONTROLLER_BENCH_CONFIG = {
    "checkpoints": [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048],
    "marination_period_s": 180,
    "sample_interval_s": 5,
}

_CONTROLLER_AUTOSCALING_CONFIG = {
    "min_replicas": 1,
    "max_replicas": 4096,
    "target_ongoing_requests": 1,
    "upscale_delay_s": 1,
}

_CONTROLLER_WAITER_TIMEOUT_S = 1200

# SignalActor from ray._common.test_utils; use high max_concurrency for many
# concurrent waiters (up to 2048+ in controller benchmark).
_SignalActorForController = _SignalActor.options(max_concurrency=100000)


@serve.deployment(
    graceful_shutdown_timeout_s=1,
    ray_actor_options={"num_cpus": 0.2},
    max_ongoing_requests=100000,
    autoscaling_config={
        "min_replicas": 5,
        "max_replicas": 10,
        "target_ongoing_requests": 100000,
        "upscale_delay_s": 1,
    },
)
class ControllerBenchHelloWorld:
    def __init__(self, signal_actor):
        self.signal = signal_actor

    async def __call__(self):
        await self.signal.wait.remote()
        return "hello"


@serve.deployment(
    autoscaling_config=_CONTROLLER_AUTOSCALING_CONFIG,
    max_ongoing_requests=2,
    graceful_shutdown_timeout_s=1,
    ray_actor_options={"num_cpus": 0.2},
)
class ControllerBenchMetricsGenerator:
    """Autoscaling deployment that generates handle metrics to stress the controller."""

    def __init__(self, hello_world: DeploymentHandle):
        self.hello_world = hello_world

    async def __call__(self):
        return await self.hello_world.remote()


def _controller_get_active_nodes() -> int:
    """Get number of active nodes in the cluster."""
    return len([n for n in ray.nodes() if n.get("Alive", False)])


async def _controller_get_replica_count(
    deployment_name: str = "ControllerBenchMetricsGenerator",
) -> int:
    """Get current number of running replicas for the specified deployment."""
    status = serve.status()
    for app in status.applications.values():
        for name, deployment in app.deployments.items():
            if name == deployment_name:
                return deployment.replica_states.get("RUNNING", 0)
    return 0


async def _controller_get_health_metrics() -> Dict[str, Any]:
    """Get controller health metrics. Fails the run if unavailable."""
    client = serve.context._global_client
    if client is None:
        raise RuntimeError(
            "Serve is not connected. get_health_metrics requires an active Serve "
            "controller. Ensure Serve is started before running the controller benchmark."
        )
    controller = client._controller
    if not hasattr(controller, "get_health_metrics"):
        raise RuntimeError(
            "Controller does not have get_health_metrics. This API is required for "
            "the controller benchmark. Please use a Ray version that supports "
            "controller health metrics."
        )
    return await controller.get_health_metrics.remote()


def _controller_extract_metrics_row(
    health_metrics: Dict[str, Any],
    checkpoint: int,
    sample: int,
    target_replicas: int,
    actual_replicas: int,
    num_nodes: int,
    autoscale_duration_s: float,
) -> Dict[str, Any]:
    """Extract a flat row from health metrics with all available fields."""

    def get_stat(d: dict, key: str, stat: str, default=0):
        return d.get(key, {}).get(stat, default)

    return {
        "checkpoint": checkpoint,
        "sample": sample,
        "target_replicas": target_replicas,
        "actual_replicas": actual_replicas,
        "num_nodes": num_nodes,
        "autoscale_duration_s": round(autoscale_duration_s, 3),
        "loop_duration_mean_s": get_stat(health_metrics, "loop_duration_s", "mean"),
        "loop_duration_std_s": get_stat(health_metrics, "loop_duration_s", "std"),
        "loops_per_second": health_metrics.get("loops_per_second", 0),
        "event_loop_delay_s": health_metrics.get("event_loop_delay_s", 0),
        "num_asyncio_tasks": health_metrics.get("num_asyncio_tasks", 0),
        "deployment_state_update_mean_s": get_stat(
            health_metrics, "deployment_state_update_duration_s", "mean"
        ),
        "application_state_update_mean_s": get_stat(
            health_metrics, "application_state_update_duration_s", "mean"
        ),
        "proxy_state_update_mean_s": get_stat(
            health_metrics, "proxy_state_update_duration_s", "mean"
        ),
        "proxy_state_update_std_s": get_stat(
            health_metrics, "proxy_state_update_duration_s", "std"
        ),
        "node_update_mean_s": get_stat(
            health_metrics, "node_update_duration_s", "mean"
        ),
        "node_update_std_s": get_stat(health_metrics, "node_update_duration_s", "std"),
        "node_update_min_s": get_stat(health_metrics, "node_update_duration_s", "min"),
        "handle_metrics_delay_mean_ms": get_stat(
            health_metrics, "handle_metrics_delay_ms", "mean"
        ),
        "replica_metrics_delay_mean_ms": get_stat(
            health_metrics, "replica_metrics_delay_ms", "mean"
        ),
        "process_memory_mb": health_metrics.get("process_memory_mb", 0),
    }


async def _controller_wait_for_replicas_up(target: int, timeout: float = 300) -> float:
    start = time.time()
    while time.time() - start < timeout:
        actual = await _controller_get_replica_count()
        if actual >= target:
            return time.time() - start
        await asyncio.sleep(0.5)
    actual = await _controller_get_replica_count()
    raise RuntimeError(
        f"Timeout: Only {actual}/{target} replicas after {timeout}s. Ending experiment."
    )


async def _controller_wait_for_replicas_down(
    target: int, timeout: float = 600
) -> float:
    start = time.time()
    while time.time() - start < timeout:
        actual = await _controller_get_replica_count()
        if actual <= target:
            return time.time() - start
        if int(time.time() - start) % 10 == 0:
            logging.info(f"  Waiting for scale down... {actual} -> {target}")
        await asyncio.sleep(1.0)
    actual = await _controller_get_replica_count()
    raise RuntimeError(
        f"Timeout: Still {actual} replicas (target {target}) after {timeout}s. "
        "Ending experiment."
    )


async def _controller_wait_for_waiters(
    signal_actor, expected: int, timeout: float = 300
) -> float:
    start = time.time()
    while time.time() - start < timeout:
        num_waiters = await signal_actor.cur_num_waiters.remote()
        if num_waiters >= expected:
            return time.time() - start
        await asyncio.sleep(0.5)
        if int(time.time() - start) % 10 == 0:
            logging.info(f"Waiting for {expected} waiters... {num_waiters}/{expected}")
    num_waiters = await signal_actor.cur_num_waiters.remote()
    raise RuntimeError(
        f"Timeout: Only {num_waiters}/{expected} requests reached replicas after "
        f"{timeout}s. Ending experiment."
    )


async def _controller_run_checkpoint(
    handle: DeploymentHandle,
    signal_actor,
    checkpoint: int,
    target_replicas: int,
    marination_period_s: int,
    sample_interval_s: int,
) -> List[Dict[str, Any]]:
    """Run a single checkpoint and collect metrics."""
    start_time = time.time()
    num_requests = int(target_replicas)

    pending_requests = [handle.remote() for _ in range(num_requests)]

    await _controller_wait_for_waiters(
        signal_actor, num_requests, timeout=_CONTROLLER_WAITER_TIMEOUT_S
    )
    await _controller_wait_for_replicas_up(
        target_replicas, timeout=_CONTROLLER_WAITER_TIMEOUT_S
    )
    autoscale_duration_s = time.time() - start_time

    samples = []
    num_samples = marination_period_s // sample_interval_s
    for sample_idx in range(num_samples):
        health_metrics = await _controller_get_health_metrics()
        actual_replicas = await _controller_get_replica_count()
        num_nodes = _controller_get_active_nodes()
        row = _controller_extract_metrics_row(
            health_metrics=health_metrics,
            checkpoint=checkpoint,
            sample=sample_idx,
            target_replicas=target_replicas,
            actual_replicas=actual_replicas,
            num_nodes=num_nodes,
            autoscale_duration_s=autoscale_duration_s,
        )
        samples.append(row)
        if sample_idx < num_samples - 1:
            await asyncio.sleep(sample_interval_s)

    await signal_actor.send.remote(clear=True)
    try:
        await asyncio.wait_for(
            asyncio.gather(*pending_requests, return_exceptions=True),
            timeout=30.0,
        )
    except asyncio.TimeoutError:
        pass

    return samples


async def run_controller_benchmark(
    config: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Run the controller health metrics benchmark and return raw samples.

    Uses MetricsGenerator (autoscaling) -> HelloWorld (fixed) -> SignalActor
    to stress the controller as replicas scale. Fails if get_health_metrics
    is unavailable.

    Args:
        config: Optional benchmark config (checkpoints, marination_period_s,
            sample_interval_s). Uses CONTROLLER_BENCH_CONFIG if None.

    Returns:
        List of sample dicts (one per marination sample). Each sample has
        target_replicas, autoscale_duration_s, loop_duration_mean_s,
        loops_per_second, event_loop_delay_s, num_asyncio_tasks, etc.
        Caller converts to perf_metrics via convert_controller_samples_to_perf_metrics.
    """
    cfg = config or CONTROLLER_BENCH_CONFIG
    checkpoints = cfg["checkpoints"]
    marination_period_s = cfg["marination_period_s"]
    sample_interval_s = cfg["sample_interval_s"]

    if not ray.is_initialized():
        ray.init()

    signal_actor = _SignalActorForController.remote()
    all_samples: List[Dict[str, Any]] = []

    try:
        for checkpoint_idx, target_replicas in enumerate(checkpoints):
            hello_world = ControllerBenchHelloWorld.bind(signal_actor)
            app = ControllerBenchMetricsGenerator.bind(hello_world)
            handle = serve.run(app, name="default", route_prefix=None)

            samples = await _controller_run_checkpoint(
                handle=handle,
                signal_actor=signal_actor,
                checkpoint=checkpoint_idx,
                target_replicas=target_replicas,
                marination_period_s=marination_period_s,
                sample_interval_s=sample_interval_s,
            )
            all_samples.extend(samples)
            serve.shutdown()
    finally:
        serve.shutdown()

    return all_samples
